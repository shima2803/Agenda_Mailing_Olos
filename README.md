# 📞 Agenda Semanal de Cobrança – Automação de Mailings Telefônicos
Ferramenta desenvolvida em Python (Tkinter + Selenium) para automatizar a geração e o envio de mailings telefônicos para a plataforma OLOS, garantindo a continuidade operacional durante o período de férias dos analistas.

---

### 🎯 Objetivo do Projeto
Com a equipe de analistas entrando de férias, surgiu a necessidade de uma solução simples e automatizada que permitisse a qualquer colaborador gerar e enviar os mailings telefônicos do dia sem conhecimento técnico ou acesso ao banco.  
Esta ferramenta cumpre exatamente esse papel: **gera o mailing completo e envia automaticamente para a OLOS com apenas um clique e o fechamento da janela**.

---

# 🖥️ Visão Geral da Aplicação
A ferramenta permite:

- Selecionar **carteira** (517, 518, 519).
- Selecionar o **mailing** do dia:
  - Quebras & Rejeitadas
  - CPC (Contato Pessoa Certa)
  - Nunca Contatados
- Executar consultas SQL no banco Gecobi.
- Gerar automaticamente o arquivo CSV formatado.
- Ao fechar a janela, abrir e logar na OLOS automaticamente.
- Enviar o arquivo gerado para importação.

Tudo isso com interface gráfica simples e intuitiva.

---

# 📂 Mailings Disponíveis

### **1. Quebras & Rejeitadas (Segunda-feira)**
Contas com acordos quebrados ou rejeitados, problemas de contato e alto potencial de recuperação.

### **2. CPC — Contato Pessoa Certa (Terça-feira)**
Foco em clientes que tiveram contato efetivo (classificação CPC) recentemente.

### **3. Nunca Contatados (Quarta-feira)**
Clientes com ausência de contato nos últimos 60 dias, visando ampliar o alcance das campanhas.

Cada mailing possui sua própria query SQL otimizada e adaptada às regras do negócio.

---

# 🗂️ Carteiras Suportadas

| Código | Nome da Carteira |
|--------|------------------|
| **517** | Itapeva Autos |
| **518** | DivZero |
| **519** | Cedidas |

O código da carteira também define o prefixo do arquivo CSV gerado.

---

# 📄 Geração Automática de CSV
Após clicar em **Gerar Mailing**, a ferramenta:

1. Executa a consulta SQL referente ao mailing escolhido.
2. Obtém todos os dados diretamente do banco Gecobi.
3. Gera um CSV no Desktop do usuário.
4. Usa o padrão:

```
AutosPF_QuebrasRejeitadas_YYYYMMDD_HHMMSS.csv
DivZeroPF_CPC_YYYYMMDD_HHMMSS.csv
CedidasPF_NuncaContatados_YYYYMMDD_HHMMSS.csv
```

Colunas sensíveis como CPF, telefones, datas e BindingID são preservadas como texto.

---

# 🤖 Envio Automático para OLOS
Ao **fechar a interface**, a automação inicia:

1. Acessa a URL da OLOS.
2. Realiza login com credenciais do arquivo SA_Credencials.txt.
3. Navega até:
   - Painel de Customizações  
   - Import/Export Web  
   - ImportFiles  
4. Seleciona **Enviar Mailing**.
5. Faz upload do CSV gerado.
6. Confirma o envio na tela de importação.

Nenhuma ação adicional do usuário é necessária.

---

# 🔑 Arquivo de Credenciais
A aplicação utiliza:

```
\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt
```

Com as chaves:

```
GECOBI_HOST=
GECOBI_USER=
GECOBI_PASS=
GECOBI_DB=
GECOBI_PORT=

OLOS_URL=
OLOS_USER=
OLOS_PASS=
```

O sistema lê esse arquivo automaticamente.

---

# 🛠 Tecnologias Utilizadas

- **Python 3**
- **Tkinter** → Interface gráfica
- **MySQL Connector** → Conexão com banco Gecobi
- **Selenium WebDriver (Chrome)** → Automação da OLOS
- **CSV Writer**
- **XPath e CSS Selectors**

---

# 🚀 Como Usar

1. Verifique se o ChromeDriver está instalado e compatível.
2. Garanta que o arquivo de credenciais está correto.
3. Execute o programa:
   ```bash
   python agenda_mailing.py
   ```
4. Na interface:
   - Escolha a carteira
   - Escolha o mailing
   - Clique **Gerar Mailing**
5. Após a mensagem de sucesso, **feche a janela**.
6. A automação abrirá a OLOS e enviará o arquivo automaticamente.

---

# 🧩 Finalidade Operacional
Este projeto foi criado **para substituir temporariamente os analistas que estarão de férias**, garantindo que:

- Os mailings telefônicos continuem sendo gerados,
- O envio diário para a OLOS não seja interrompido,
- Qualquer colaborador consiga executar o processo sem dificuldades.

---

# 👨‍💻 Autor
Ferramenta desenvolvida para garantir continuidade e eficiência operacional na rotina de cobrança, com foco em simplicidade, automação e confiabilidade.

---
