def create_and_upload_file_google_drive(file_name, file_content, folder_id, drive):

    file = drive.CreateFile({'title': f'{file_name}', "parents":  [{"id": folder_id}]})
    file.SetContentString(file_content)
    file.Upload()


def create_folder_on_google_drive(folder_name, drive):
    folders = drive.ListFile({
        "q": "mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList()
    folder_titles_to_id = {}
    for folder in folders:
        folder_titles_to_id[folder['title']] = folder['id']
    if folder_name not in folder_titles_to_id.keys():
        folder = drive.CreateFile({'title': folder_name, 'mimeType': 'application/vnd.google-apps.folder'})
        folder.Upload()
        return folder['id']
    else:
        return folder_titles_to_id[folder_name]