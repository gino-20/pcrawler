from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import urllib3
import os
from multiprocessing import Pool
import pickle
import argparse
import hashlib


class GetPypi:
    def __init__(self):
        self.threads = 10
        self.file_list = []
        self.depth = 1
        self.download_folder = args.local_folder
        self.buffer = 4096
        self.session_exists = False
        self.show_logo()
        self.get_package_names()

    def show_logo(self):
        logo = """
 _____     _____ _      _               _           _         
|  _  |_ _|  _  |_|   _| |___ _ _ _ ___| |___ ___ _| |___ ___ 
|   __| | |   __| |  | . | . | | | |   | | . | .'| . | -_|  _|
|__|  |_  |__|  |_|  |___|___|_____|_|_|_|___|__,|___|___|_|  
      |___|                                                   
        """
        print(logo)

    def get_package_names(self):
        if os.path.exists('session'):
            print('Found previous session. Do you want to resume? (y/n)')
            while (job := input()) not in ['y', 'n']:
                print('Choose wisely! (y/n)')
            if job == 'y':
                try:
                    print('Loading previous session...')
                    with open('session', 'rb') as session:
                        self.file_list = pickle.load(session)
                    self.session_exists = True
                except Exception as e:
                    print(f'Error {e} loading previous session! Exiting...')
                    return
                else:
                    print('Successfully loaded previous session!')
                    # The session is loaded, time to rescan downloaded packages
                    # Remove Nones from file_list
                    print(f'Cleaning non-existent packages (if any)...')
                    none_count = self.file_list.count(None)
                    for i in tqdm(range(none_count), total=none_count):
                        self.file_list.remove(None)
                    print('Done!')
                    self.download_folder_scan()
            else:
                print('Proceeding to download...')
        if not self.session_exists:
            print(f'Queuing PyPi base url...')
            page = requests.get(base_url+base_dir)
            print(f'Got code {page.status_code}')
            print('Parsing package list. This could take a while...')
            soup = BeautifulSoup(page.text, 'lxml')
            total_packages = len(soup.find_all('a', href=True))
            print(f'Total {total_packages} found...')
            print('Building list of files to fetch....')
            parsed_links = [i['href'] for i in soup.find_all('a', href=True)]
            with Pool(processes=8) as p:
                self.file_list = list(tqdm(p.imap(self.get_package_files, parsed_links),
                                           total=total_packages, unit=' packages'))
            # Remove Nones from file_list
            print(f'Cleaning non-existent packages (if any)...')
            none_count = self.file_list.count(None)
            for i in tqdm(range(none_count), total=none_count):
                self.file_list.remove(None)
            print('Done')
            with open('session', 'wb') as flist:
                print('Backing up package list...')
                pickle.dump(self.file_list, flist)
                print('Done!')
        if os.path.exists(self.download_folder):
            print('Download folder exists!')
            self.download_folder_scan()
        else:
            try:
                os.mkdir(self.download_folder)
                print('Successfully created download folder!')
            except Exception as e:
                print(f'Error {e} creating download folder! Exiting...')
                return
        with Pool(processes=8) as p:
            r = list(tqdm(p.imap(self.download_thread, self.file_list), total=len(self.file_list), unit=' files'))

    def download_folder_scan(self):
        print('Scanning download folder for downloaded packages...')
        for package in self.file_list:
            package_name = package[0].split('/')[-2]
            if os.path.exists(self.download_folder + package_name):
                print(f'Folder for {package_name} exists, checking file...')
                if os.path.exists(self.download_folder + package_name + package[0].split('/')[-1]):
                    print(f'Found package file! Removing it from the download queue...')
                    self.file_list.remove(package)
                else:
                    print('Package file not found!')

    def get_package_files(self, package):
        current_list = {}
        try:
            package_dir = requests.get(base_url + package)
        except Exception as e:
            print(f'Cathed an error {e}, trying to index {package}')
            print('Skipping to the next package...')
            return
        soup = BeautifulSoup(package_dir.text, 'lxml')
        # Building tar.gz list for current package...
        for link in soup.find_all('a', href=True):
            # We have to separate hash
            test_link = link['href']
            if test_link.split('#')[0].endswith('tar.gz'):
                # Append link and the file hash to local dict
                current_list.update({test_link.split('#')[0]: test_link.split('#')[1]})
            if len(current_list) > 0:
                return [list(current_list.items())[-1][0], list(current_list.items())[-1][1]]

    def download_thread(self, link):
        http = urllib3.PoolManager()
        fname = link[0].split('/')[-1]
        r = http.request('GET', link[0], preload_content=False)
        try:
            with open(self.download_folder + fname, 'wb') as file:
                while True:
                    t_data = r.read(self.buffer)
                    if not t_data:
                        break
                    file.write(t_data)
                r.release_conn()
        except KeyboardInterrupt:
            print('Download aborted! Removing partial downloaded file...')
            os.remove(self.download_folder + fname)
            print(f'File {fname} successfully deleted!')
        else:
            if args.check_hash:
                file_hash = hashlib.sha256()
                try:
                    with open(self.download_folder + fname, 'rb') as file:
                        for chunk in iter(lambda: file.read(4096), b''):
                            file_hash.update(chunk)
                except FileNotFoundError:
                    print(f'Error opening package file {self.download_folder}')
                else:
                    if file_hash.hexdigest() != int(link[1].split('=')[-1]):
                        print(f'Digest error for {fname}')
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download latest packackages from PyPi repository',
                                     usage='By default downloads all packages to ./PyPi folder')
    parser.add_argument('--local_folder', type=str, help='Folder to store packages', default='./PyPi/')
    parser.add_argument('--check_hash', action='store_true', help='Use to check sha256 digest')
    args = parser.parse_args()
    base_url = 'https://pypi.org'
    base_dir = '/simple/'
    GetPypi()
