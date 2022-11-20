import argparse
import csv
import os
import time
import requests


from concurrent.futures import ThreadPoolExecutor, wait
from bs4 import BeautifulSoup
from random import choice
from environs import Env
import pandas as pd

env = Env()
env.read_env()


def create_argument_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--csv_url',
        help='Укажите ссылку на csv, в котором лежат url-ы страниц с хабра',
        nargs='?',
        default='https://drive.google.com/file/d/1oIAXcPzyxpTEj5hdZ8bZPA4TPVX9k_nC/view?usp=share_link',
        type=str
    )
    parser.add_argument(
        '--dest_folder',
        help='Укажите директорию, в которую сохранить результаты парсинга',
        nargs='?',
        default=os.getcwd(),
        type=str
    )

    return parser


def get_proxy():

    response = requests.get('https://free-proxy-list.net/')
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, 'lxml')

    trs = soup.find('table', class_='table table-striped table-bordered').find_all('tr')[1:51]

    proxies = []

    for tr in trs:
        tds = tr.find_all('td')
        ip = tds[0].text.strip()
        port = tds[1].text.strip()
        if 'yes' in tds[6].text.strip():
            proxy = {'schema': 'https', 'address': ip + ':' + port}
            proxies.append(proxy)
    return proxies


def parse_data_from_habr(url, user_agent, proxy):

    # proxy_ip_port = {proxy['schema']: proxy['address']}

    headers = {
        'User-Agent': user_agent
    }
    proxies = {
        'http': env.str('SOCKS5_PROXY'),
        'https': env.str('SOCKS5_PROXY')
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    page_html = response.text
    soup = BeautifulSoup(page_html, 'lxml')
    username = soup.select_one('.page-title__title').text
    return url, username, page_html


def read_csv_from_google_drive(url):
    path = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
    user_urls = pd.read_csv(path).values
    return user_urls


def upload_data_to_csv(user_link, user_agent, proxy):
    try:
        url, username, page_html = parse_data_from_habr(user_link[0],
                                                        user_agent,
                                                        proxy)
        with open(f'Страницы с хабра/{username}.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerows([[url], [username], [page_html]])
        time.sleep(1)
    except Exception as e:
        print(e)


def main():

    parser = create_argument_parser()
    args = parser.parse_args()
    user_agents = open('user_agents.txt').read().split('\n')
    futures = []
    os.makedirs(f'{args.dest_folder}/Страницы с хабра', exist_ok=True)

    # for user_link in read_csv_from_google_drive(args.csv_url):
    #     upload_data_to_csv(user_link, choice(user_agents), 'choice(proxies)')
    with ThreadPoolExecutor() as executor:
        for user_link in read_csv_from_google_drive(args.csv_url):
            futures.append(
                executor.submit(upload_data_to_csv,
                                user_link,
                                choice(user_agents),
                                'choice(proxies)')
            )
    wait(futures)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))

