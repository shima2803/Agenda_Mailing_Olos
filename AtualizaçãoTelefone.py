import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

CRED_PATH = r"\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt"
URL_LOGIN = "http://192.168.0.251/gecobi2/qfrontend/#/auth/login"


def load_cpj_credentials(path=CRED_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    cpj_user = None
    cpj_pass = None

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if key == "CPJ_USER":
                cpj_user = value
            elif key == "CPJ_PASS":
                cpj_pass = value

    if not cpj_user or not cpj_pass:
        raise ValueError("CPJ_USER e/ou CPJ_PASS não encontrados no arquivo de credenciais.")

    return cpj_user, cpj_pass


def abrir_gecobi_com_cpj():
    usuario, senha = load_cpj_credentials()

    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get(URL_LOGIN)

    wait = WebDriverWait(driver, 30)

    campo_usuario = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "input[aria-label='Usuário']"))
    )
    time.sleep(0.5)
    campo_usuario.clear()
    campo_usuario.send_keys(usuario)
    campo_usuario.send_keys(Keys.TAB)

    campo_senha = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "input[aria-label='Senha']"))
    )
    time.sleep(0.5)
    campo_senha.clear()
    campo_senha.send_keys(senha)
    campo_senha.send_keys(Keys.TAB)

    time.sleep(0.5)
    campo_senha.send_keys(Keys.ENTER)

    botao_calculate = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//i[contains(@class,'material-icons') and normalize-space()='calculate']"))
    )
    time.sleep(0.5)
    botao_calculate.click()

    botao_operacao = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'q-item__label') and normalize-space()='Operação']"))
    )
    time.sleep(0.5)
    botao_operacao.click()

    botao_telefones = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'q-item__section') and contains(@class,'q-item__section--main') and normalize-space()='Telefones']"))
    )
    time.sleep(0.5)
    botao_telefones.click()

    return driver


driver = abrir_gecobi_com_cpj()

input("Pressione ENTER para manter o programa aberto...")
