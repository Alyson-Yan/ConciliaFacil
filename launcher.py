
"""
Script de inicialização para o aplicativo de Conciliação
Autor: Yan Fernandes
Descrição: codigo que da o comando "streamlit run" para o app.py, permitindo que o aplicativo seja executado em um ambiente de produção.
"""

#launcher.py
import subprocess
import os

# Garante que o caminho funcione mesmo depois de empacotar
script_path = os.path.join(os.path.dirname(__file__), 'app.py')

# Executa o comando streamlit
subprocess.run(["streamlit", "run", script_path])
