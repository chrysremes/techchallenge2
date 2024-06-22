from bolsa_b3.defs_bolsa_b3custom import BolsaB3
from file_handling.file_handling_b3custom_class import FileHandlerB3Custom

bolsa_b3 = BolsaB3()
bolsa_b3.set_new_B3_url()
print(bolsa_b3.B3_url)

df = bolsa_b3.scrap_to_df(bolsa_b3.B3_url)

print(df)