# Git + GitHub Básico (Linux)

## Instalar Git

```bash
sudo apt update
sudo apt install git -y
```

Verificar instalação:

```bash
git --version
```

---

# Configuração inicial

Configurar nome:

```bash
git config --global user.name "Seu Nome"
```

Configurar email:

```bash
git config --global user.email "seuemail@gmail.com"
```

Verificar:

```bash
git config --global --list
```

---

# Clonar repositório

```bash
git clone https://github.com/USUARIO/REPOSITORIO.git
```

Entrar na pasta:

```bash
cd REPOSITORIO
```

---

# Verificar branch atual

```bash
git branch
```

---

# Verificar remoto

```bash
git remote -v
```

---

# Adicionar arquivos

Adicionar tudo:

```bash
git add .
```

Adicionar arquivo específico:

```bash
git add arquivo.py
```

---

# Fazer commit

```bash
git commit -m "mensagem do commit"
```

Exemplo:

```bash
git commit -m "add airflow architecture"
```

---

# Enviar para GitHub

```bash
git push origin main
```

---

# Caso dê conflito

Sobrescrever remoto:

```bash
git push --force origin main
```

---

# Atualizar projeto local

```bash
git pull origin main
```

---

# Criar nova branch

```bash
git checkout -b feature/nova-feature
```

---

# Trocar branch

```bash
git checkout main
```

---

# Ver status

```bash
git status
```

---

# Ver histórico

```bash
git log
```

---

# Estrutura recomendada para projetos

```text
meu_projeto/
│
├── airflow/
├── spark_jobs/
├── api/
├── docker/
├── docs/
├── tests/
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

# Criar .gitignore

```bash
touch .gitignore
```

Exemplo:

```text
.venv/
__pycache__/
*.log
*.csv
*.parquet
.env
node_modules/
```

---

# Remover tudo do repositório mantendo Git

```bash
find . -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} \;
```

---

# Adicionar nova pasta

```bash
mv ~/Downloads/nova_pasta .
```

Depois:

```bash
git add .
git commit -m "add new folder"
git push origin main
```

---

# Renomear remote origin

```bash
git remote rename origin github
```

Push depois:

```bash
git push github main
```

---

# Criar Token GitHub

Abrir:

https://github.com/settings/tokens

Criar:

- Generate new token (classic)
- Marcar: repo

Usar token como password no terminal.

---

# SSH (recomendado)

Gerar chave:

```bash
ssh-keygen -t ed25519 -C "seuemail@gmail.com"
```

Mostrar chave:

```bash
cat ~/.ssh/id_ed25519.pub
```

Adicionar no GitHub:

https://github.com/settings/keys

Testar:

```bash
ssh -T git@github.com
```

Usar URL SSH:

```bash
git@github.com:USUARIO/REPOSITORIO.git
```

---

# Fluxo mais comum

```bash
git add .
git commit -m "mensagem"
git push origin main
```

---

# Comandos úteis

Ver diferenças:

```bash
git diff
```

Remover arquivo do Git:

```bash
git rm arquivo.txt
```

Renomear arquivo:

```bash
mv antigo.py novo.py
```

---

# Boas práticas

- Um repo por sistema/projeto grande
- Não usar branch para separar arquitetura
- Não subir datasets gigantes
- Não subir .venv
- Usar README.md
- Fazer commits pequenos e organizados

---

# Exemplo profissional

```text
data-platform/
│
├── airflow/
├── spark/
├── docker/
├── infra/
├── scripts/
├── docs/
└── README.md
```
