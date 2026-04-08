# Documentação — Sessão Git (backend-carga)

---

## Informações do Repositório

| Item | Valor |
|---|---|
| **URL** | `https://git.cnj.jus.br/datalake/pessoas/backend-carga.git` |
| **Branch principal** | `master` |
| **Autor** | zhang yuan (`zhang.yuan@cnj.jus.br`) |
| **Token de acesso** | `glpat-k8DT49Ezx7_Nsm8xCkw1` |

---

## Branches disponíveis

master
remotes/origin/HEAD -> origin/master
remotes/origin/QA
remotes/origin/develop
remotes/origin/develop-bkp
remotes/origin/master
text
text

---

## Comandos executados

### 1. Clonar o repositório

```bash
git clone https://git.cnj.jus.br/datalake/pessoas/backend-carga.git

Após executar, um painel de autenticação será exibido para completar os dados de login.



2. Listar branches

bash
bash
git branch -a


3. Erro — Diretório incorreto

text
text
fatal: not a git repository (or any of the parent directories): .git

Causa: O terminal não estava dentro do diretório do repositório clonado.


Solução:


bash
bash
cd ~/Desktop/sadads/backend-carga


4. Verificar log de commits

bash
bash
git log

Resultado:


text
text
commit 574914798f50f1acb0b2b3b146e6e8c737b91fa0
Author: zhang yuan <zhang.yuan@cnj.jus.br>
Date:   Mon Feb 17 00:09:44 2025 +0000

    Update main.py


5. Reverter commit (merge revert)

bash
bash
git revert -m 1 574914798f50f1acb0b2b3b146e6e8c737b91fa0

Resultado — Conflito detectado:


text
text
Auto-merging main.py
CONFLICT (content): Merge conflict in main.py
error: could not revert 5749147... Update main.py

hint: After resolving the conflicts, mark them with
hint: "git add/rm <pathspec>", then run
hint: "git revert --continue".
hint: You can instead skip this commit with "git revert --skip".
hint: To abort and get back to the state before "git revert",
hint: run "git revert --abort".


6. Resolver conflito

Opção A — Via VS Code

1.Abrir o arquivo main.py no VS Code
2.Procurar os marcadores de conflito:

python
python
<<<<<<< HEAD
código atual (antes do merge)
=======
código do commit que está sendo revertido
>>>>>>> 5749147... Update main.py

3.Escolher qual versão manter e remover os marcadores
4.Salvar o arquivo

Opção B — Via terminal

bash
bash
# Após resolver o conflito manualmente
git add main.py
git revert --continue

Opção C — Abortar o revert

bash
bash
git revert --abort

Opção D — Pular o commit

bash
bash
git revert --skip


7. Confirmar o revert resolvido

bash
bash
git revert 574914798f50f1acb0b2b3b146e6e8c737b91fa0


8. Verificar log após o revert

bash
bash
git log

Resultado:


text
text
commit f1de8204b8bdbdd742175d32e2e81b4687fa3d35
Author: zhang yuan <zhang.yuan@cnj.jus.br>
Date:   Wed Feb 19 09:38:06 2025 +0000


9. Sair do editor (vim/less)

text
text
:q


Fluxo resumido

text
text
git clone
    │
    ▼
cd backend-carga
    │
    ▼
git log  ──►  identificar commit problemático
    │
    ▼
git revert -m 1 <hash>
    │
    ├── Conflito? ──►  resolver no VS Code
    │                      │
    │                      ▼
    │                 git add main.py
    │                 git revert --continue
    │
    ▼
git log  ──►  confirmar revert
    │
    ▼
git push origin master  (enviar para o servidor)


Comandos auxiliares AWS S3

bash
bash
aws s3 ls s3://datalake-cnj-stg/dados/pessoas


Checklist rápido

 Clonar repositório
 Listar branches
 Identificar commit problemático (5749147)
 Executar git revert
 Resolver conflito em main.py
 Confirmar revert
 Push para o servidor remoto

