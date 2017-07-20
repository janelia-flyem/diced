import unittest
import tempfile
import shutil

from diced import DicedStore
from diced import DicedException

class TestDicedStore(unittest.TestCase):
    def test_initserver(self):
        """Check running of default dvid server and potential errors.
        """

        defstore2 = None
        dbdir = tempfile.mkdtemp()
        dbdir2 = tempfile.mkdtemp()

        defstore = DicedStore(dbdir)
        try:

            # dvid address
            self.assertEqual(defstore._server, "127.0.0.1:8000")
    
            # try running DVID on same address
            try:
                defstore = DicedStore(dbdir)
            except DicedException:
                pass # Good, should not be able to create duplicate store
            else:
                assert False, "Expected an exception above, but it wasn't raised!"
           
            # use previous address
            reusestore = DicedStore("dvid://127.0.0.1", port=8000, 
                    rpcport=8001)
            self.assertEqual(reusestore._server, "127.0.0.1:8000")
    
            # run a second dvid server
            defstore2 = DicedStore(dbdir2, port=9000, rpcport=9001)
    
            # dvid address
            self.assertEqual(defstore2._server, "127.0.0.1:9000")      
        
        finally:
            # shutdown DVID 
            defstore._shutdown_store()
            if defstore2:
                defstore2._shutdown_store()
        
            # cleanup dirs
            shutil.rmtree(dbdir)
            shutil.rmtree(dbdir2)

    def test_repos(self):
        """Test the creation, deletion, and querying of repos.
        """
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir, port=10000, rpcport=10001)

        try:
            store.create_repo("myrepo")
            store.create_repo("myrepo1")
    
            # make sure duplicate repos cannot be added
            try:
                store.create_repo("myrepo1")
            except DicedException:
                pass # Good, should not be able to create duplicate repo
            else:
                assert False, "Expected an exception above, but it wasn't raised!"
    
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

        finally:
            store._shutdown_store()
            shutil.rmtree(dbdir)

if __name__ == "main":
    unittest.main()
