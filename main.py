from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import urllib3
import os
from multiprocessing import Pool
import pickle


class GetPypi:
    def __init__(self):
        self.threads = 10
        self.file_list = []
        self.depth = 1
        self.download_folder = './'
        self.buffer = 4096
        self.get_package_names()

    def show_logo(self):
        pass

    def get_package_names(self):
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
            self.file_list = list(tqdm(p.imap(self.get_package_files, parsed_links), total = total_packages, unit = ' packages'))
        with open('session', 'wb') as flist:
            print('Backing up package list...')
            pickle.dump(self.file_list, flist)
            print('Done!')
        with Pool(processes=8) as p:
            r = list(tqdm(p.imap(self.download_thread, self.file_list), total=len(self.file_list), unit=' files'))

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
        # if args.verbose:
        #    print(f'Downloading {fname}, size: {r.info()["Content-Length"]}')
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
            # Need to add checksum verification!!!
            return


if __name__ == '__main__':
    base_url = 'https://pypi.org'
    base_dir = '/simple/'
    GetPypi()
