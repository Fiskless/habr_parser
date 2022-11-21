import argparse
import csv
import os
import time
import requests
import random

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


class HabrParser():

    def __init__(self, url):
        self.url = url

    @staticmethod
    def parse_data_from_habr(user_link, user_agent):

        headers = {
            'User-Agent': user_agent
        }
        proxies = {
            'http': env.str('SOCKS5_PROXY', None),
            'https': env.str('SOCKS5_PROXY', None)
        }

        response = requests.get(user_link, proxies=proxies, headers=headers)
        response.raise_for_status()
        page_html = response.text
        soup = BeautifulSoup(page_html, 'lxml')
        username = soup.select_one('.page-title__title').text
        return user_link, username, page_html

    def read_csv_from_google_drive(self):
        path = 'https://drive.google.com/uc?id=' + self.url.split('/')[-2]
        user_urls = pd.read_csv(path).values
        return user_urls

    def upload_data_to_csv(self, user_link, user_agent, csv_with_errors):
        try:
            url, username, page_html = self.parse_data_from_habr(user_link[0],
                                                                 user_agent)
            with open(f'Страницы с хабра/{username}.csv', 'w') as f:
                writer = csv.writer(f)
                writer.writerows([[url], [username], [page_html]])
            time.sleep(random.uniform(1, 1.5))
        except Exception as e:
            csv_with_errors.writerows([[user_link[0], e]])


def main():

    parser = create_argument_parser()
    args = parser.parse_args()

    with open('user_agents.txt') as f:
        user_agents = f.read().split('\n')

    os.makedirs(f'{args.dest_folder}/Страницы с хабра', exist_ok=True)

    parser_errors = open(f'Ошибки при парсинге.csv', 'w')
    csv_with_errors = csv.writer(parser_errors)
    csv_with_errors.writerows([["Ссылка на пользователя", "Ошибка"]])

    habr_parser = HabrParser(args.csv_url)

    futures = []
    with ThreadPoolExecutor() as executor:
        for user_link in habr_parser.read_csv_from_google_drive():
            futures.append(
                executor.submit(habr_parser.upload_data_to_csv,
                                user_link,
                                choice(user_agents),
                                csv_with_errors)
            )
    wait(futures)

    parser_errors.close()

    if sum(1 for row in open("Ошибки при парсинге.csv")) == 1:
        os.remove('Ошибки при парсинге.csv')


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))

