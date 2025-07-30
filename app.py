import streamlit as st
import os
import sys
from enum import Enum
import psutil
# Configuração da página com mais opções
st.set_page_config(
    page_title="Sistema de Conciliação Bancária",
    layout="centered",
    page_icon="🏦",
    initial_sidebar_state="expanded"
)

# CSS aprimorado com temas e responsividade
st.markdown("""
    <style>
    /* MODIFICAÇÃO: As variáveis de cores primária e secundária foram removidas
    e o estilo do botão foi alterado para usar um gradiente cinza diretamente.
    */
    
    /* Botões (agora todos em cinza) */
    div[data-testid="stButton"] > button {
        background: linear-gradient(135deg, #6b728000, #9ca3af);
        color: white;
        border: none;
        border-radius: 100px;
        padding: 0.75em 1em;
        font-size: 16px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    div[data-testid="stButton"] > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        opacity: 0.95;
    }
    
    /* MODIFICAÇÃO: A regra específica para o botão 'Voltar' foi removida, 
    pois o estilo principal já contempla a cor cinza para todos os botões.
    */
    
    /* Títulos */
    h1 {
        color: #4f46e5; /* Cor primária mantida para o título */
    }
    
    /* Divisor personalizado */
    hr {
        border: 1px solid #e5e7eb;
        margin: 1.5rem 0;
    }
    
    /* Mensagens de informação */
    .stAlert {
        border-radius: 12px;
    }
    
    @media (max-width: 768px) {
        div[data-testid="stButton"] > button {
            width: 100%;
            margin-bottom: 0.5rem;
        }
    }
    </style>
""", unsafe_allow_html=True)

class Banco(Enum):
    """Enum para os bancos disponíveis"""
    SANTANDER = "santander"
    CIELO = "cielo"
    CREDSHOP = "credshop" 


def main():
    """Função principal do aplicativo"""
    st.title("🪙 Concilia Fácil")
    st.markdown("---")
    
    # Verifica se o banco foi selecionado usando uma abordagem mais robusta
    if 'banco_selecionado' not in st.session_state or st.session_state.banco_selecionado not in [b.value for b in Banco]:
        mostrar_tela_inicial()
    else:
        carregar_modulo_banco()

# Função para obter o caminho absoluto correto para os recursos
def caminho_absoluto_relativo(relativo):
    try:
        base_path = sys._MEIPASS  # Quando empacotado pelo PyInstaller
    except AttributeError:
        base_path = os.path.abspath(".")  # Execução normal

    caminho_direto = os.path.join(base_path, relativo)

    # Caso o logos esteja dentro de _internal (ex: launcher/_internal/logos/)
    if not os.path.exists(caminho_direto):
        caminho_internal = os.path.join(base_path, "_internal", relativo)
        if os.path.exists(caminho_internal):
            return caminho_internal

    return caminho_direto


def mostrar_tela_inicial():
    """Exibe a tela de seleção de banco inicial"""
    st.subheader("Escolha o Banco para conciliação")

    # Santander
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image(caminho_absoluto_relativo("logos/santander.png"), width=70)
    with col2:
        if st.button("💳 Santander", key="btn_santander", use_container_width=True):
            st.session_state.banco_selecionado = Banco.SANTANDER.value
            st.rerun()

    # Cielo
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image(caminho_absoluto_relativo("logos/cielo.png"), width=70)
    with col2:
        if st.button("💳 Cielo", key="btn_cielo", use_container_width=True):
            st.session_state.banco_selecionado = Banco.CIELO.value
            st.rerun()

    # Credshop
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image(caminho_absoluto_relativo("logos/credshop.png"), width=70)
    with col2:
        if st.button("💳 Credshop", key="btn_credshop", use_container_width=True):
            st.session_state.banco_selecionado = Banco.CREDSHOP.value
            st.rerun()

    st.info("Selecione um banco para iniciar o processo de conciliação.")



def carregar_modulo_banco():
    """Carrega o módulo específico do banco selecionado"""
    # O botão de voltar agora herdará o novo estilo cinza padrão
    st.button("🔙 Voltar", key="btn_voltar", on_click=resetar_app, use_container_width=True)
    
    # Divisor visual
    st.markdown("---")
    
    # Carrega o módulo correspondente
    try:
        if st.session_state.banco_selecionado == Banco.SANTANDER.value:
            from santander import main as santander_main
            santander_main()
        elif st.session_state.banco_selecionado == Banco.CIELO.value:
            from cielo import main as cielo_main
            cielo_main()
        elif st.session_state.banco_selecionado == Banco.CREDSHOP.value:
            from credshop import main as credshop_main
            credshop_main()

    except ImportError as e:
        st.error(f"Erro ao carregar módulo: {str(e)}. Certifique-se de que o arquivo do banco existe (ex: santander.py).")
        resetar_app()

def resetar_app():
    """Reseta o aplicativo para o estado inicial"""
    # Guarda o valor do banco selecionado antes de limpar, se necessário
    banco_selecionado_antes = st.session_state.get('banco_selecionado', None)

    # Limpa todo o estado da sessão
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    
    # Adicionado para evitar que a tela pisque ou tente recarregar um módulo
    if 'banco_selecionado' in st.session_state:
        del st.session_state['banco_selecionado']


if __name__ == "__main__":
    main()