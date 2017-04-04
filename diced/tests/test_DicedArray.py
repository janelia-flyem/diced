import unittest
import tempfile
import shutil
import numpy

from diced import DicedStore
from diced import DicedException
from diced import ArrayDtype
from libdvid import DVIDNodeService

class TestDicedArray(unittest.TestCase):
    def test_arraysmall(self):
        """Tests small get/puts.

        This test creates 3D array and tests get/set.
        """
        # create store object and repo
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir)
        # my initial repo
        store.create_repo("myrepo")
        myrepo = store.open_repo("myrepo")

        # test generic 3D raw array type
        arr = myrepo.create_array("myarray", ArrayDtype.uint64, islabel3D=True)
        self.assertEqual(arr.get_numdims(), 3)
        extents = arr.get_extents()
        self.assertEqual(extents, (slice(0,0),slice(0,0),slice(0,0)))

        # set and get data
        data = numpy.zeros((400,200,100), numpy.uint64)
        data[:] = 5
        arr[1:401,2:202,3:103] = data
        matchdata = arr[1:401,2:202,3:103]
        matches = numpy.array_equal(data, matchdata)
        self.assertTrue(matches)

        extents = arr.get_extents()
        self.assertEqual(extents, (slice(0,448),slice(0,256),slice(0,128)))

        # check negative coordinates
        arr[-3,-1,4] = numpy.array([[[121]]])
        val = arr[-3,-1,4] 
        self.assertEqual(121, val)

        # check array access
        arr[-3,-1,3:5] = numpy.array([[[121,122]]], numpy.uint64)
        val = arr[-3,-1,3:5] 
        matches = numpy.array_equal(numpy.array([121,122], numpy.uint64), val)
        self.assertTrue(matches)




        # check error cases
        founderror = False
        try:
            b = arr[0,5]
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        store._shutdown_store()

    def test_array1d2d(self):
        """Tests small get/puts on 1D/2D arrays.

        This test creates 1D/2D array and tests get/set.
        """

        # create store object and repo
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir)
        # my initial repo
        store.create_repo("myrepo")
        myrepo = store.open_repo("myrepo")

        # test generic 2D raw array type
        arr = myrepo.create_array("myarray2d", ArrayDtype.uint8, dims=2)
        self.assertEqual(arr.get_numdims(), 2)
        extents = arr.get_extents()
        self.assertEqual(extents, (slice(0,0),slice(0,0)))

        # set and get data
        data = numpy.zeros((200,100), numpy.uint8)
        data[:] = 5
        arr[2:202,3:103] = data
        matchdata = arr[2:202,3:103]
        matches = numpy.array_equal(data, matchdata)
        self.assertTrue(matches)

        extents = arr.get_extents()
        self.assertEqual(extents, (slice(0,512),slice(0,512)))

        # check error cases
        founderror = False
        try:
            b = arr[0]
        except DicedException:
            founderror = True
        self.assertTrue(founderror)

        # test generic 1D raw array type
        arr = myrepo.create_array("myarray1d", ArrayDtype.uint8, dims=1)
        self.assertEqual(arr.get_numdims(), 1)
        extents = arr.get_extents()
        self.assertEqual(extents, slice(0,0))

        # set and get data
        data = numpy.zeros((100), numpy.uint8)
        data[:] = 5
        arr[3:103] = data
        matchdata = arr[3:103]
        matches = numpy.array_equal(data, matchdata)
        self.assertTrue(matches)

        extents = arr.get_extents()
        self.assertEqual(extents, slice(0,262144))

        # check error cases
        founderror = False
        try:
            b = arr[0:3, 0:3]
        except DicedException:
            founderror = True
        self.assertTrue(founderror)


        store._shutdown_store()


    def test_arraylarge(self):
        """Tests large get/puts.

        This test creates large 3D array and tests get/set.
        """
        
        # create store object and repo
        dbdir = tempfile.mkdtemp()
        store = DicedStore(dbdir)
        # my initial repo
        store.create_repo("myrepo")
        myrepo = store.open_repo("myrepo")

        # test generic 3D raw array type
        arr = myrepo.create_array("myarray", ArrayDtype.uint8)

        # ?! test negative coord

        b = ArrayDtype.uint8

        # set and get data
        #data = numpy.random.randint(1031, size=(1400,1200,1000)).astype(numpy.uint8)
        data = numpy.zeros((1400,1200,1000), numpy.uint8)
        data[:] = 3

        arr[1:1401,2:1202,3:1003] = data
        
        matchdata = arr[1:1401,2:1202,3:1003]
        matches = numpy.array_equal(data, matchdata)
        self.assertTrue(matches)
        
        matchzero = arr[1401,1202,1003]
        self.assertEqual(0, matchzero)

        extents = arr.get_extents()
        self.assertEqual(extents, (slice(0,1408),slice(0,1216),slice(0,1024)))

        store._shutdown_store()

if __name__ == "main":
    unittest.main()
