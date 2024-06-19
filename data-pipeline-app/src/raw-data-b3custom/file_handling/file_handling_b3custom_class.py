import urllib.request
import zipfile
import os

class FileHandlerB3Custom():

    def __init__(self, url:str) -> None:
        self.url = url
        self.file_handle = None
        self.unzipped_file = None
        pass

    def url_file_request(self):
        try:
            print("URL Requested")
            filehandle, _ = urllib.request.urlretrieve(self.url)
            print("OK - File handled")
            self.file_handle = filehandle
        except:
            raise Exception("Bad URL request")
        
    def url_file_download(self,filename:str, path_name:str):
        if not (path_name == ""):
            if not os.path.exists("./"+path_name):
                os.makedirs("./"+path_name)
        fullfilename = os.path.join(path_name, filename)
        try:
            print("URL Requested to Download")
            urllib.request.urlretrieve(self.url, fullfilename)
            print("OK - File handled")
        except:
            raise Exception("Bad URL request")

    def unzip_file_to_ram(self):
        try:
            zip_file_object = zipfile.ZipFile(self.file_handle, 'r')
            all_files = zip_file_object.namelist()
            read_file = []
            print("Unzipping files")
            for current_file in all_files:
                print("For loop Unzipping")
                file = zip_file_object.open(current_file)
                read_file.append(file.read())
            print("Finished")
            return read_file
        except:
            raise Exception("Error while Unzipping file")
        
    def unzip_file_to_disk(self, dir_name:str):
        try:
            print("Unzipping files")
            zip_file_object = zipfile.ZipFile(self.file_handle, 'r')
            zip_file_object.extractall(dir_name) # extract file to dir
            print("Finished")
        except:
            raise Exception("Error while Unzipping file")
        