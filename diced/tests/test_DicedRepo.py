import unittest
import tempfile
import shutil
import os
import time

from diced import DicedStore
from diced import DicedException

class TestDicedRepo(unittest.TestCase):
    def test_repoversion(self):
        """Check versioning in repos.

        This test creates a repo and creates different versions.
        """
        # create store object and repo
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir)
        # my initial repo
        store.create_repo("myrepo")
        myrepo = store.open_repo("myrepo")

        # check current version
        uuid = myrepo.get_current_version()
        uuid2 = store.list_repos()[0][1]
        self.assertEqual(uuid, uuid2)

        # check lock status
        self.assertFalse(myrepo.locked)

        # check branch on open node is illegal
        founderror = False
        try:
            myrepo.create_branch("start")
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        # lock node
        myrepo.lock_node("now done")
        self.assertTrue(myrepo.locked)

        # create new branch
        uuidnew = myrepo.create_branch("start")
        self.assertNotEqual(uuid, uuidnew) 

        # open repo with uuid
        myrepov2= store.open_repo(uuid=uuidnew)
        self.assertEqual(myrepov2.get_current_version(), uuidnew)
        
        # change version
        myrepo.change_version(uuidnew)
        self.assertFalse(myrepo.locked)
        self.assertEqual(myrepo.uuid, uuidnew)
        
        store._shutdown_store()
        shutil.rmtree(dbdir)

    def test_files(self):
        """Test file interface in DicedRepo.

        This test adds files, retrieves files, and delete files
        from the version repo.
        """
        # create store object and repo
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir)
        # my initial repo
        store.create_repo("myrepo")
        myrepo = store.open_repo("myrepo")

        # create file
        myrepo.upload_filedata("hello.txt", "world text")
        res = myrepo.download_filedata("hello.txt")
        self.assertEqual(res, "world text")
        
        # overwrite
        myrepo.upload_filedata("hello.txt", "world text2")
        res = myrepo.download_filedata("hello.txt")
        self.assertEqual(res, "world text2")

        # new file
        myrepo.upload_filedata("world.txt", "foobar")

        # test file list
        allfiles = myrepo.list_files() 
        self.assertEqual(len(allfiles), 2)
        self.assertTrue("hello.txt" in allfiles)
        self.assertTrue("world.txt" in allfiles)

        # test error handling
        founderror = False
        try:
            res = myrepo.download_filedata("hello2.txt")
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        rootuuid = myrepo.get_current_version()

        myrepo.lock_node("lock")

        # check lock error message
        founderror = False
        try:
            myrepo.upload_filedata("world.txt", "foobar")
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        # create new branch and overwrite file data
        newuuid = myrepo.create_branch("new branch")
        myrepo.change_version(newuuid)

        # modify data in child node
        myrepo.upload_filedata("world.txt", "newdata")
        res = myrepo.download_filedata("world.txt")
        self.assertEqual(res, "newdata")
      
        myrepo.delete_file("hello.txt")
        allfiles = myrepo.list_files() 
        self.assertEqual(len(allfiles), 1)
        self.assertTrue("world.txt" in allfiles)

        # text not overwritten in root
        myrepo.change_version(rootuuid)
        res = myrepo.download_filedata("world.txt")
        self.assertEqual(res, "foobar")

        # file shouldn't be deleted in root
        allfiles = myrepo.list_files() 
        self.assertEqual(len(allfiles), 2)

        store._shutdown_store()


if __name__ == "main":
    unittest.main()
