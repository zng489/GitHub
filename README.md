# GitHub

```
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git --version
git version 2.33.1.windows.1

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git help init

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Acessing the account!! 

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git config --global user.email "zhang489@hotmail.com"

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Creating file

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ touch test.txt

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Editing

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ git config --global core.editor "notepad"

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Opening the file

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ notepad test.txt

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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

# Creating New One Folder

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ mkdir shop

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github
$ cd shop

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop
$ git init
Initialized empty Git repository in C:/Users/Yuan/Desktop/github/shop/.git/

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

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ touch lists.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ rm lists.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ ls

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ touch list.txt

Yuan@DESKTOP-G41O7CK MINGW64 ~/Desktop/github/shop (master)
$ ls
list.txt


```
