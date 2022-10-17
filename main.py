from base64 import encode
from utils.aws_utils import *
from datetime import datetime
from io import BytesIO
import pandas as pd

dados = []

for i in range(650000):
    dados.append({f'Usuario {str(i)}', f'Senha {str(i)}'})
df_usuarios = pd.DataFrame(dados, columns=['username', 'senha'])

buffer = BytesIO()
df_usuarios.to_parquet(buffer, engine='auto')
buffer.seek(0)

aws_util = aws_utils()
print(f'inicio: {datetime.now()}')

aws_util.upload(bucket='', key='usuarios_parquet_hoje',
                file=buffer)
print(f'fim: {datetime.now()}')
