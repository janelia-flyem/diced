import unittest
import tempfile
import shutil
import os
import time

from diced import DicedStore
from diced import DicedException

class TestDicedStore(unittest.TestCase):
    def test_initserver(self):
        """Check running of default dvid server and potential errors.
        """

        dbdir = tempfile.mkdtemp()
        defstore = None 
        caughterror = False
        try:
            defstore = DicedStore(dbdir)
        except DicedException:
            # catch any runtime error for creation
            caughterror = True
        self.assertFalse(caughterror)

        # dvid address
        self.assertEqual(defstore._server, "127.0.0.1:8000")

        # try running DVID on same address
        caughterror = False
        try:
            defstore = DicedStore(dbdir)
        except DicedException:
            # catch error for double creation
            caughterror = True
        self.assertTrue(caughterror)
       
        # use previous address
        reusestore = DicedStore("dvid://127.0.0.1", port=8000, 
                rpcport=8001)
        self.assertEqual(reusestore._server, "127.0.0.1:8000")



        # run a second dvid server
        dbdir2 = tempfile.mkdtemp()
        defstore2 = None 
        caughterror = False
        try:
            defstore2 = DicedStore(dbdir2, port=9000, rpcport=9001)
        except DicedException:
            # catch any runtime error for creation
            caughterror = True
        self.assertFalse(caughterror)

        # dvid address
        self.assertEqual(defstore2._server, "127.0.0.1:9000")      
        
        # shutdown DVID 
        defstore._shutdown_store()
        defstore2._shutdown_store()
        
        # cleanup dirs
        shutil.rmtree(dbdir)
        shutil.rmtree(dbdir2)

    def test_repos(self):
        """Test the creation, deletion, and querying of repos.
        """
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir, port=10000, rpcport=10001)

        store.create_repo("myrepo")
        store.create_repo("myrepo1")

        # make sure duplicate repos cannot be added
        caughterror = False
        try:
            store.create_repo("myrepo1")
        except DicedException:
            caughterror = True
        self.assertTrue(caughterror)

        # grab repo names
        reponames = store.list_repos()
        self.assertEqual(len(reponames), 2)
        nameonly = []
        for (name, uuid) in reponames:
            nameonly.append(name)
        self.assertTrue("myrepo" in nameonly)
        self.assertTrue("myrepo1" in nameonly)
        
        # delete repo and check repo list
        store.delete_repo("myrepo")
        reponames = store.list_repos()
        self.assertEqual(len(reponames), 1)
        nameonly = []
        for (name, uuid) in reponames:
            nameonly.append(name)
        self.assertFalse("myrepo" in nameonly)
        self.assertTrue("myrepo1" in nameonly)

        #  test get uuid interface
        uuid = store.get_repouuid("myrepo1")
        self.assertEqual(uuid, reponames[0][1])

        # retrieve DicedRepo object
        repo = store.open_repo("myrepo1") 

        store._shutdown_store()
        shutil.rmtree(dbdir)

if __name__ == "main":
    unittest.main()
