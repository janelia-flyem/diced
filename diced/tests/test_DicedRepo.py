import unittest
import tempfile
import shutil
import os
import time
import json

from diced import DicedStore
from diced import DicedException
from diced import ArrayDtype
from libdvid import DVIDNodeService

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

    def test_array(self): 
        """Test array creation/loading interface.

        Tries creating different types of arrays,
        checks proper deletion, and checks error
        handling.
        """

        # create store object and repo
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir)
        # my initial repo
        store.create_repo("myrepo")
        myrepo = store.open_repo("myrepo")

        # test generic 3D raw array type
        arr = myrepo.create_array("myarray", ArrayDtype.uint8)
        self.assertEqual(arr.numdims, 3)
        self.assertFalse(arr.islabel3D)
        self.assertFalse(arr.locked)

        arr = myrepo.get_array("myarray")
        self.assertEqual(arr.numdims, 3)
        self.assertFalse(arr.islabel3D)
        self.assertFalse(arr.locked)


        # test 2D raw array type
        arr = myrepo.create_array("myarray2d", ArrayDtype.uint8, dims=2)
        self.assertEqual(arr.numdims, 2)
        self.assertFalse(arr.islabel3D)
        self.assertFalse(arr.locked)

        arr = myrepo.get_array("myarray2d")
        self.assertEqual(arr.numdims, 2)
        self.assertFalse(arr.islabel3D)
        self.assertFalse(arr.locked)
        

        # check 3D labels
        arr = myrepo.create_array("mylabelarray", ArrayDtype.uint64, islabel3D=True)
        self.assertEqual(arr.numdims, 3)
        self.assertTrue(arr.islabel3D)
        self.assertFalse(arr.locked)
        
        arr = myrepo.get_array("myarray2d")
        self.assertEqual(arr.numdims, 2)
        

        # query instances
        instances = myrepo.list_instances()
        self.assertEqual(len(instances), 3)
        
        allinstances = myrepo.list_instances(showhidden=True)
        self.assertEqual(len(allinstances), 5)


        # test delete
        myrepo.delete_array("myarray2d")
        instances = myrepo.list_instances()
        self.assertEqual(len(instances), 2)

        # reinsert and change meta and lossy
        arr = myrepo.create_array("myarray2d", ArrayDtype.uint8, dims=3, lossycompression=True)
        self.assertEqual(arr.numdims, 3)
        self.assertFalse(arr.islabel3D)
        self.assertFalse(arr.locked)

        # test hidden files set in meta
        ns = DVIDNodeService("127.0.0.1:8000", myrepo.get_current_version())
        ns.create_keyvalue("blahblah")
       
        # reload myrepo
        myrepo.change_version(myrepo.get_current_version())
        allinstances = myrepo.list_instances(showhidden=True)
        self.assertEqual(len(allinstances), 6)
        self.assertTrue(("blahblah", "keyvalue") in allinstances) 

         
        # test hidden exclusion add
        ns.put(".meta", "restrictions", json.dumps(["myarray2d"]))
        myrepo.change_version(myrepo.get_current_version())
        allinstances = myrepo.list_instances()
        self.assertEqual(len(allinstances), 2)
    

        # test array errors
        founderror = False
        try:
            arr = myrepo.create_array("myarray", ArrayDtype.uint8)
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        founderror = False
        try:
            arr = myrepo.create_array("myarray", ArrayDtype.uint16, islabel3D=True)
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        founderror = False
        try:
            arr = myrepo.get_array("myarray3")
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        # try to create array on locked node
        myrepo.lock_node("blah")
        try:
            arr = myrepo.create_array("myarray4", ArrayDtype.uint16)
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        # check lock status
        arr = myrepo.get_array("myarray2d")
        self.assertEqual(arr.numdims, 3)
        self.assertFalse(arr.islabel3D)
        self.assertTrue(arr.locked)

        store._shutdown_store()


if __name__ == "main":
    unittest.main()
