# 📞 Agenda Semanal de Cobrança – Automação de Mailings Telefônicos

Ferramenta desenvolvida em Python (Tkinter + Selenium) para automatizar a geração e o envio de mailings telefônicos para a plataforma OLOS com filtro opcional de Portfolio (infoad) e novos tipos de mailing.

---
## ✅ Versões do Projeto (Final e Compilada)

Este repositório possui duas versões oficiais:

VerFinal.py → versão final do código-fonte (para manutenção, ajustes e auditoria).

VerFinal.zip → versão já compilada (pronta para uso operacional, sem precisar instalar Python).

### 📌 Recomendação

Uso diário na operação: VerFinal.zip

Ajustes e evolução do projeto: VerFinal.py

---
## 🎯 Objetivo do Projeto

Com a equipe entrando em férias, surgiu a necessidade de uma solução simples e automatizada que permitisse a qualquer colaborador gerar e enviar os mailings telefônicos sem conhecimento técnico ou acesso ao banco.

Esta ferramenta cumpre exatamente esse papel:

Gera o mailing automaticamente

Aplica filtros opcionais por infoad

Formata o CSV corretamente

Abre e envia para a OLOS com Selenium

Todo o processo ocorre com 1 clique e o fechamento da janela

---
## 🖥️ Visão Geral da Aplicação

A aplicação permite:

Selecionar a carteira (517, 518, 519)

Selecionar o mailing desejado

Aplicar filtro opcional por Portfolio (infoad)

Executar consultas SQL completas diretamente no banco

Gerar automaticamente o CSV no Desktop

Enviar automaticamente para a OLOS ao fechar a interface

Interface projetada para ser simples, rápida e acessível.

---

## 📂 Mailings Disponíveis
1. Quebras & Rejeitadas (Segunda-feira)

Contas com acordos quebrados ou rejeitados, priorizando recuperação imediata.

2. CPC — Contato Pessoa Certa (Terça-feira)

Clientes com histórico recente de contato efetivo (status CPC).

3. Nunca Contatados (Quarta-feira)

Clientes sem qualquer contato nos últimos 60 dias.

4. Mailing Geral (Quinta-feira) — Novo

Traz toda a carteira, sem restrições. Ideal para campanhas amplas.

5. Base Recente (Sexta-feira) — Novo

Somente cadastros novos, inseridos nos últimos 2 meses (data_cad = data_arq).

--- 
## 🗂️ Carteiras Suportadas
Código	Nome da Carteira
517	Itapeva Autos
518	DivZero
519	Cedidas

O código selecionado determina o prefixo do arquivo gerado.

### 🔍 Filtro Opcional por Portfolio (infoad) — Novo

A aplicação agora carrega automaticamente todos os infoads do banco:

SELECT DISTINCT infoad 
FROM cadastros_tb
WHERE cod_cli IN (517, 518, 519)
ORDER BY 1;

Como funciona

Se nenhum infoad for selecionado → Mailing traz toda a carteira

Se 1 infoad for selecionado → Filtra apenas esse grupo

Se vários forem selecionados → Aplica IN (...) automaticamente na SQL

Todos os valores recebem escape de segurança para evitar erros SQL.

---

## 📄 Geração Automática de CSV

Após clicar em Gerar Mailing, o sistema:

Executa a SQL correspondente ao mailing

Aplica, se houver, o filtro por infoad

Gera um CSV no Desktop

Nomeia automaticamente no formato:

AutosPF_QuebrasRejeitadas_20250101_101500.csv
DivZeroPF_CPC_20250101_101500.csv
CedidasPF_NuncaContatados_20250101_101500.csv

### ✔ Novo comportamento

Se infoads forem selecionados, eles são incluídos no nome:

AutosPF_Geral_BradescoIV_BradescoVII_20250101_101500.csv


Telefones, CPF e datas são preservados como texto.
---
## 🤖 Envio Automático para OLOS

Ao fechar a janela, a automação:

Abre o Chrome

Acessa a URL da OLOS

Faz login automaticamente

Navega até ImportFiles

Faz upload do arquivo

Confirma o envio

Nenhuma intervenção manual é necessária.
---
## 🔑 Arquivo de Credenciais

O sistema utiliza:

\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt


## 🛠 Tecnologias Utilizadas

Python 3

Tkinter (GUI)

MySQL Connector

Selenium WebDriver + ChromeDriver

CSV Writer

XPath / CSS Selectors

---
# 🚀 Como Usar
### ✅ Opção 1 — Versão Compilada (Recomendada)

Baixe/extraia o arquivo VerFinal.zip

Execute o aplicativo compilado

Escolha:

Carteira

Tipo de mailing

(Opcional) Infoads

Clique em Gerar Mailing

Após a mensagem de sucesso, feche a janela

A automação irá iniciar o envio para a OLOS

### 🧑‍💻 Opção 2 — Rodando pelo Código-Fonte

Certifique-se de que o ChromeDriver é compatível com seu Chrome

Verifique o arquivo de credenciais

Execute o programa:

python VerFinal.py


Escolha:

Carteira

Tipo de mailing

(Opcional) Infoads

Clique em Gerar Mailing

Após a mensagem de sucesso, feche a janela

A automação irá iniciar o envio para a OLOS

## 🧩 Finalidade Operacional

Criado para garantir que:

Os mailings telefônicos continuem rodando diariamente

O processo não dependa de analistas especializados

Qualquer colaborador consiga utilizá-lo com segurança

A operação continue mesmo durante períodos de férias

Automação robusta, simples e confiável.

## 👨‍💻 Autor

Ferramenta desenvolvida com foco em eficiência, simplicidade e segurança operacional, garantindo a continuidade da operação de cobrança.
