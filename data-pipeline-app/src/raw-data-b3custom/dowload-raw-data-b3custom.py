from bolsa_b3.defs_bolsa_b3custom import BolsaB3
from file_handling.file_handling_b3custom_class import FileHandlerB3Custom

date_to_download = '2024-06-14'

FILES_PATH = "./archives/"
DIR_NAME_TO_EXTRACT = FILES_PATH+"extracted"
FILE_NAME_TO_DOWNLOAD = "B3_NEGOCIACOES_"+date_to_download+".zip"

bolsa_b3 = BolsaB3(date_to_download)
B3_url = bolsa_b3.get_B3_url()
print(B3_url)

file_handler_b3 = FileHandlerB3Custom(B3_url)
# file_handler_b3.url_file_request()
# # unzipped_file = file_handler_b3.unzip_file_to_ram()
# file_handler_b3.unzip_file_to_disk(DIR_NAME_TO_EXTRACT)

file_handler_b3.url_file_download(FILE_NAME_TO_DOWNLOAD,FILES_PATH)