''' 
Ths purpose of this script is to automate some of the Git Hub procedures through GitPython package instead of Gitbash

Main dependency is GitPython 

'''
import git
from git import Repo

# Global control vars + initiate local repository
global_vars = {
    'local_dir': 'C:/git/macro_dash_database',  # Folder on your machine
    'remote_URL': 'https://github.com/jsong6688/macro_dash_database.git', # Remote git repository URL
    'set_up_local': False,  # Switch for setting up local, linking to remote repo and tracking master branch on local directory
    'update_local': True,  # Switch for updating local copy of the repository. WARNING: Will automatically update your local copy to the latest remote - no warning
    'commit_push':  True   # Switch for checking new local additions vs. reomte repository, commit + comment and push to repository
}   

# Initialise a local repository - this creates the 'local_dir' folder 
macro_db_repo_local = Repo.init(global_vars['local_dir']) #This sets up a Repo object at local repository directory

if global_vars['set_up_local']:
    # Code to setup the new macro database repository

    # Add a new remote - this is the first time setting up this repository so add remote origin
    remote_origin = macro_db_repo_local.create_remote('origin', url = global_vars['remote_URL']) # This hooks up with remote repository

    assert remote_origin.exists() # Check it's actually been created..

    remote_origin.fetch() # Fetch latest info on the remote repository - for exapmle what branches are present

    # Set-up remote tracking by the local repository/branch to the remote repository/branch
    macro_db_repo_local.create_head('master', remote_origin.refs.master) # Create a pointer - current branch
    macro_db_repo_local.heads.master.set_tracking_branch(remote_origin.refs.master) #Set local branch "master" to track remote "master"
    macro_db_repo_local.heads.master.checkout() # Checkout local "master" to working tree

    # Pull from remote repository
    macro_db_repo_local.remotes.origin.pull()

if global_vars['update_local']:
    # Code to update and pull from remote

    # First fetch differences from remote
    macro_db_repo_local.remotes.origin.fetch()  

   # Pull from remote repository after fetching 
    macro_db_repo_local.remotes.origin.pull()


if global_vars['commit_push']:
    # Code for tracking, commiting and pushing local changes / additions to remote repository

    # Get a list of untracked files - this shows new files we've added locally
    additions = macro_db_repo_local.untracked_files
    # print(additions)

    # Now commit all new additions in the local repository
    for new_fle in additions:
        print(new_fle + " has been added and is now added to the index")
        macro_db_repo_local.index.add(new_fle)
    
    # Now commit all modified fles in the local repository
    for mod_fle in macro_db_repo_local.index.diff(None):
        print(mod_fle.a_path + " has been modified and is now added to the index")
        macro_db_repo_local.index.add(mod_fle.a_path)
    
    macro_db_repo_local.index.commit("Commiting newly added and modified files")

    # Push all new commits on the local repository / branch
    macro_db_repo_local.remotes.origin.push()
