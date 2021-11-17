# GitHub

```
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git --version
git version 2.33.1.windows.1

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##### Git Help!!!! #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git --help
usage: git [--version] [--help] [-C <path>] [-c <name>=<value>]
           [--exec-path[=<path>]] [--html-path] [--man-path] [--info-path]
           [-p | --paginate | -P | --no-pager] [--no-replace-objects] [--bare]
           [--git-dir=<path>] [--work-tree=<path>] [--namespace=<name>]
           [--super-prefix=<path>] [--config-env=<name>=<envvar>]
           <command> [<args>]

These are common Git commands used in various situations:

start a working area (see also: git help tutorial)
   clone             Clone a repository into a new directory
   init              Create an empty Git repository or reinitialize an existing one

work on the current change (see also: git help everyday)
   add               Add file contents to the index
   mv                Move or rename a file, a directory, or a symlink
   restore           Restore working tree files
   rm                Remove files from the working tree and from the index
   sparse-checkout   Initialize and modify the sparse-checkout

examine the history and state (see also: git help revisions)
   bisect            Use binary search to find the commit that introduced a bug
   diff              Show changes between commits, commit and working tree, etc
   grep              Print lines matching a pattern
   log               Show commit logs
   show              Show various types of objects
   status            Show the working tree status

grow, mark and tweak your common history
   branch            List, create, or delete branches
   commit            Record changes to the repository
   merge             Join two or more development histories together
   rebase            Reapply commits on top of another base tip
   reset             Reset current HEAD to the specified state
   switch            Switch branches
   tag               Create, list, delete or verify a tag object signed with GPG

collaborate (see also: git help workflows)
   fetch             Download objects and refs from another repository
   pull              Fetch from and integrate with another repository or a local branch
   push              Update remote refs along with associated objects

'git help -a' and 'git help -g' list available subcommands and some
concept guides. See 'git help <command>' or 'git help <concept>'
to read about a specific subcommand or concept.
See 'git help git' for an overview of the system.

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##### It will take you to git webpage #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git help init

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

###### Acessing the account!! #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git config --global user.email "zhang489@hotmail.com"

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##### Creating file #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ touch test.txt


###### Editing #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git config --global core.editor "notepad"


##### Opening the file #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ notepad test.txt

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##### Config List #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git config --list
diff.astextplain.textconv=astextplain
filter.lfs.clean=git-lfs clean -- %f
filter.lfs.smudge=git-lfs smudge -- %f
filter.lfs.process=git-lfs filter-process
filter.lfs.required=true
http.sslbackend=openssl
http.sslcainfo=C:/Program Files/Git/mingw64/ssl/certs/ca-bundle.crt
core.autocrlf=true
core.fscache=true
core.symlinks=false
pull.rebase=false
credential.helper=manager-core
credential.https://dev.azure.com.usehttppath=true
init.defaultbranch=master
user.email=zhang489@hotmail.com
core.editor=notepad

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##### Creating New One Folder (In this case the folder 'shop') #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ mkdir shop

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ cd shop

##### Creating git init #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop
$ git init
Initialized empty Git repository in C:/Users/Yuan/Desktop/github/shop/.git/

##### Checking the directory #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ ls -A
.git/

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ cd .git

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop/.git (GIT_DIR!)
$ ls -A
HEAD  config  description  hooks/  info/  objects/  refs/

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop/.git (GIT_DIR!)
$ cd ..

##### Creating Files #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ touch lists.txt

##### Removing and Deleting Files #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ rm lists.txt

##### Checking the directory #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ ls

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ touch list.txt

##### Checking the directory in the shop folder #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ ls
list.txt

##### Git Status #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git status
lsOn branch master

No commits yet

Untracked files:
  (use "git add <file>..." to include in what will be committed)
        list.txt

nothing added to commit but untracked files present (use "git add" to track)

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

##### Adding your file #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git add list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git status
On branch master

No commits yet

Changes to be committed:
  (use "git rm --cached <file>..." to unstage)
        new file:   list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git commit -m 'Creating shopping list'
[master (root-commit) 71f483b] Creating shopping list
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git status
On branch master
nothing to commit, working tree clean

##### log (data to identify your files) #####

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git log
commit 71f483bfa7c02424a6eb6043ab40957b1156d5b2 (HEAD -> master)
Author: unknown <zhang489@hotmail.com>
Date:   Mon Nov 15 22:34:40 2021 -0300

    Creating shopping list


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ notepad list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git status
On branch master
Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
        modified:   list.txt

Untracked files:
  (use "git add <file>..." to include in what will be committed)
        lists.txt

no changes added to commit (use "git add" and/or "git commit -a")

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git diff
diff --git a/list.txt b/list.txt
index e69de29..e8e0963 100644
--- a/list.txt
+++ b/list.txt
@@ -0,0 +1,4 @@
+Bananas
+Oranges
+Strawberries
+Apples
\ No newline at end of file

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git add list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git status
On branch master
Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
        modified:   list.txt

Untracked files:
  (use "git add <file>..." to include in what will be committed)
        lists.txt


Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git commit -m 'Add Fruit and Vegetables'
[master 66a8a39] Add Fruit and Vegetables
 1 file changed, 4 insertions(+)

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git log
commit 66a8a39c54ceb24e8d8972603f7ab3e33772027b (HEAD -> master)
Author: unknown <zhang489@hotmail.com>
Date:   Tue Nov 16 11:02:56 2021 -0300

    Add Fruit and Vegetables

commit 71f483bfa7c02424a6eb6043ab40957b1156d5b2
Author: unknown <zhang489@hotmail.com>
Date:   Mon Nov 15 22:34:40 2021 -0300

    Creating shopping list

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git diff

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git diff --staged
diff --git a/list.txt b/list.txt
index e8e0963..52742b5 100644
--- a/list.txt
+++ b/list.txt
@@ -1,4 +1,5 @@
 Bananas
 Oranges
 Strawberries
-Apples
\ No newline at end of file
+Apples
+zhang
\ No newline at end of file

-------------------------------------------------------------------------------------------------------------------------------------------------------------
Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ git checkout HEAD~1 list.txt
Updated 1 path from b50a881
----------------------------------------------------------------------------------------------

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git init
Initialized empty Git repository in C:/Users/Yuan/Desktop/github/.git/

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ touch list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ notepad list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git add list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git status
On branch master

No commits yet

Changes to be committed:
  (use "git rm --cached <file>..." to unstage)
        new file:   list.txt

Untracked files:
  (use "git add <file>..." to include in what will be committed)
        shop/


Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git diff -staged
error: invalid option: -staged

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git diff --staged
diff --git a/list.txt b/list.txt
new file mode 100644
index 0000000..b2798b0
--- /dev/null
+++ b/list.txt
@@ -0,0 +1,2 @@
+zhang
+yuan
\ No newline at end of file

---------------------------------------
Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git brach dairy

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git brach 
  dairy
* master

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git brach meat
  dairy
* master
  meat
  
Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git checkout dairy
Switched to brach 'dairy'

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ notapad list.txt
  
Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ notapad list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git add list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git commit -m 'Update'

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (dairy)
$ git checkout master

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git checkout meat
Switched to branch 'meat'

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git merge meat
Updatind 940e4..
Fast-forward
 list.txt | 5 +++++
 1 file changed, 5 insertions(+)
 
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git help remote

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git remote add origin https://.....

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git remote -v
origin https://..... (fetch)
origin https://..... (push)

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git push origin master

-----------------------------------------------------------------------

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ cd..

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ mkdir imaging_data/

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ cd imaging_data/

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github (master)
$ git clone https://....



```
