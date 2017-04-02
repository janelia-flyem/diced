"""Functonality to access a specific version of a repository.
"""

import numpy as np
from libdvid import DVIDNodeService, ConnectionMethod, DVIDConnection
from libdvid._dvid_python import DVIDException
from enum import Enum

class ArrayDtype(Enum):
    """Defines datatypes supported.
    """
    uint8 = np.uint8
    uint16 = np.uint16
    uint32 = np.uint32
    uint64 = np.uint64

class DicedRepo(object):
    """Provides access to a version of the specified repo.

    Note:
        If the version of the repo has been locked (i.e., is not open),
        creating datainstance and writing data is disabled.  If one
        wants to write to the repo, a new node should be branched.
    """ 
    
    # block size constants
    BLKSIZE3D = 64
    BLKSIZE2D = 512
    BLKSIZE1D = 262144
   
    # keep track of all internal DVID datatypes supported
    SupportedTypes = {"uint8blk" : ArrayDtype.uint8, "uint16blk": ArrayDtype.uint16,
            "uint32blk": ArrayDtype.uint32, "uint64blk": ArrayDtype.uint64,
            "labelblk": ArrayDtype.uint64}
    RawTypeMappings = {ArrayDtype.uint8: "uint8blk", ArrayDtype.uint16: "uint16blk",
                        ArrayDtype.uint32: "uint32blk", ArrayDtype.uint64: "uint64blk"} 
    LabelTypeMappings = {ArrayDtype.uint8: "labelblk", ArrayDtype.uint16: "labelblk",
                        ArrayDtype.uint32: "labelblk", ArrayDtype.uint64: "labelblk"} 

    
    LabelTypes = set(["labelblk"])
   
    MetaLocation = ".meta"
    FilesLocation = ".files"
    RestrictionName = "restrictions"
    InstanceMetaPrefix = "instance/"

    def __init__(self, server, uuid, dicedstore):
        self.server = server
        # hold reference
        self.dicedstore = dicedstore
        self.uuid = None
        # DVID connections
        self.nodeconn = None
        self.rawconn = None

        # all instances to ignore for ls
        self.hidden_instances = None

        # fetch all repo information
        self.repoinfo = None
        # whether current version is read only
        self.locked = None
       
        # meta for current node
        self.current_node = None

        # instances available at version
        self.allinstances = None
        self.activeinstances = None
        
        # initialize version specific data
        self._init_version(uuid)

        # create meta types if not currently available
        if MetaLocation not in self.activeinstances:
            self.nodeconn.create_keyvalue(MetaLocation)
        if FileLocation not in self.activeinstances:
            self.nodeconn.create_keyvalue(FileLocation)


    def change_version(self, uuid):
        """Change the current node version.

        Args:
            uuid (str): version node to use
        
        Raises:
            DicedException if UUID not found
        """
        
        for tuuid in self.alluids:
            if tuuid.startswith(uuid):
                self._init_version(uuid) 
        raise DicedException("UUID not found")

    def get_current_version(self, uuid):
        """Retrieve the current version ID.

        Returns:
            String for UUID
        """

        return self.uuid

    def get_array(self, name):
        """Retrive a DicedArray object.

        Returns:
            DicedArray

        Raises:
            DicedException if the array does not exist
        """

        if name in self.activeInstances:
            # check if accepted type
            typename = self.activeInstances[name]
            if typename in SupportedTypes: # only support arrays
                islabel3D = typename in LabelTypes
                numdims = 3

                # check if num dims and user dtype specified
                try:
                    rootuuid = self.currentnode["DataInstances"][name]["Base"]["DataUUID"]
                    data = self.nodeconn.get_json(MetaLocation, self.InstanceMetaPrefix+name+":"+rootuuid) 
                    numdims = data["numdims"]
                except:
                    pass

                return DicedArray(name, self.dicedstore, self.locked, self.nodeconn, 
                    self.currentnode, numdims, dtype, islabel3D)
            else:
                raise DicedException("Instance name: " + name + " has an unsupported type " + typename)
        
        raise DicedException("Instance name: " + name + " not found in version " + self.uuid)


    def create_array(name, dtype, dims=3, islabel3D=False, lossycompression=False, versioned=True): 
        """Create a new array in the repo at this version.

        Args:
            name (str): unique name for this array
            dtype (ArrayDtype): datatype (not relevant if labels)
            dims (int): number of dimensions (support 1,2,3)
            islabel3D (bool): treat as label data (usually highly compressible) (always 64bit)
            versioned (bool): allow array to be versioned
            lossycompresion (bool): use lossy compression (only if not islabel3D)

        Returns:
            new DicedArray object

        Raises:
            DicedException if node is locked or name is already used.
        """

        if self.locked:
            raise DicedException("Cannot create instance on locked node")
       
        for (tname, uuid) in self.allinstances:
            if tname == name:
                raise DicedException("Name already exists in repo")

        if islabel3D and dims != 3:
            raise DicedException("islabel3D only supported for 3D data")
        
        if islabel3D and dtype != ArrayDtype.uint8:
            raise DicedException("islabel3D only works with 64 bit data")

        conn = DVIDConnection(dvid_server) 

        endpoint = "/repo/" + self.uuid + "/instance"
        blockstr = "%d,%d,%d" % (blocksize[2], blocksize[1], blocksize[0])
        
        typename = RawTypeMappings[dtype]
        if islabel3D:
            typename = LabelTypeMappings[dtype]

        # handle blocksize
        blockstr = str(BLKSIZED3D) + "," + str(BLKSIZED3D) + "," + str(BLKSIZED3D)
        if dims == 2:
            blockstr = str(BLKSIZED2D) + "," + str(BLKSIZED2D) + "," + str(1)
        if dims == 1:
            blockstr = str(BLKSIZED1D) + "," + str(1) + "," + str(1)

        data = {"typename": typename, "dataname": name, "BlockSize": blockstr}
        
        if not islabels3D and lossycompression:
            data["Compression"] = "jpeg"

        conn.make_request(endpoint, ConnectionMethod.POST, json.dumps(data))

        # use '.meta' keyvalue to store array size (since not internal to DVID yet) 

        self.nodeconn.put(MetaLocation, InstanceMetaPrefix+name+":"+self.uuid, json.dumps({"numdims": dims}))

        # update current node meta 
        self._init_version(self.uuid)

        return DicedArray(name, self.dicedstore, False, self.nodeconn, 
            self.currentnode, dims, dtype, islabel3D)

    def upload_filedata(self, dataname, data):
        """Upload file data to this repo version.

        Args:
            dataname (str): name of file
            data (str): data to store
        """

        self.nodeconn.put(FileLocation, dataname, data)


    def download_filedata(self, dataname):
        """Download file data.

        Args:
            dataname (str): name of file
           
        Returns:
            Data for file as a string

        Raises:
            DicedException if file does not exist
        """

        data = None
        try:
            data = self.nodeconn.get(Filelocation, dataname)
        except DVIDException:
            raise DicedException("file does not exist")

    def list_instances(self, showhidden=False):
        """Lists data store in this version of the repo.

        This will show all the array data.  The user can
        choose to show everything shown in DVID.

        Args:
            showhidden (boolean): show all instances (even non-array)
       
        Return:
            [ (instance name, ArrayType) ] if showhidden is True
            the function returns [ (instance name, type name string) ].

        """
        
        res = []
        for instance, typename in self.activeinstances.items():
            if showhidden:
                res.append((instance, typename))
            elif typename in self.SupportedTypes and instance not in self.hidden_instances:
                res.append(instance, self.SupportedTypes[typename])
    
        return res

    def list_files(self):
        """List all the files for this version node.
        
        Returns:
            List of strings for file names
        """

        return self.nodeconn.custom_request("/" + FileLocation + "/keys/0/z",
            ConnectionMethod.GET)

    def create_branch(self, message):
        """Create a new branch from this locked node.

        Args:
            message (str): commit message
        
        Returns:
            string for new version UUID            

        Raises:
            DicedExcpetion if node is not locked.
        """
        
        if not self.locked:
            raise DicedException("Must lock node before branching")
        
        res = self.nodeconn.custom_request("/branch",
                json.dumps({"note": message}), ConnectionMethod.POST)

        #  add new uuid (no need to reinit everything)
        self.alluuids.add(res["child"])

        return res["child"]
        

    def lock_node(self, message):
        """Lock node.

        Args:
            message (str): commit message
        
        Returns:
            string for new version UUID            

        Raises:
            DicedExcpetion if node is already locked.
        """
        
        if self.locked:
            raise DicedException("Node already locked")
        
        self.nodeconn.custom_request("/commit",
                json.dumps({"note": message}), ConnectionMethod.POST)

        # no need to reinit everything
        self.locked = True
   
    def delete_file(self, filename):
        """Delete file from this version.

        This will only delete the file from the current version.
        
        Args:
            filename (str): name of file
        """

        self.nodeconn.custom_request("/" + FilesLocation + "/key/" + filename, 
                "", ConnectionMethod.DELETE)
        
    def delete_array(self, dataname):
        """Delete array from repo (not just version!) -- this cannot be undone!

        Note:
            For large arrays this could be very time-consuming.
            While this is non-blocking, currently, DVID
            will not resume the deletion on restart, so it is
            possible for data to still be stored even if it
            is superficially removed.  For now, the user should
            ensure DicedStore is open for a some time after
            issue the command.

        TODO:
            Implement a restartable background delete.

        """

        deletecall = subprocess.Popen(['dvid', 'repo', self.uuid, 'delete', dataname], stdout=None)
        deletecall.communicate()

    def _init_version(self, uuid):
        # fetch all repo information
        self.repoinfo = json.loads(conn.make_request("/repo/" + uuid + "/info", ConnectionMethod.GET))
        self.allinstances = {}
        # load all versions in repo
        self.alluuids = set()
        dag = self.repoinfo["DAG"]["Nodes"]
        for uuid, nodedata in dag.items():
            self.alluuids.add(nodedata["UUID"])
        
        for instancename, val in self.repoinfo["DataInstances"].items():
            # name is not necessarily unique to a repo
            self.allinstances[(instancename, val["Base"]["DataUUID"])] = val["Base"]["TypeName"]
        
        # create connection to repo
        self.nodeconn = DVIDNodeService((str(server), str(uuid)))
        self.rawconn = DVIDConnection(server) 

        # datainstances that should be hidden (array of names) 
        self.hidden_instances = set(self.nodeconn.getjson(MetaLocation, RestrictionName))
        
        nodeids = {}
        # check if locked note
        dag = self.repoinfo["DAG"]["Nodes"]
        for tuuid, nodedata in dag.items():
            nodeids[nodedata["VersionId"]] = nodedata
            if tuuid.startswith(uuid):
                self.uuid = str(tuuid)
                self.current_node = nodedata
                self.locked = nodedata["Locked"]
           
        # load all ancestors
        ancestors = set()
        currnode = self.currentnode
        while True:
            ancestors.add(currnode["UUID"])
            if len(self.currentnode["Parents"]) > 0:
                currnode = nodeids[self.currentnode["Parents"][0]] 
            else:
                break

        # load all instances
        self.activeinstances = {}

        for instancename, val in self.repoinfo["DataInstances"].items():
            if val["Base"]["DataUUID"] in ancestors:
                self.activeinstances[instancename] = val["Base"]["TypeName"]

