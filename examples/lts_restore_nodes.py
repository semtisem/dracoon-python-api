import asyncio
import logging
import sys

from typing import List, Dict, Optional
from pydantic import BaseModel
from dracoon import DRACOON, OAuth2ConnectionType
from dracoon.errors import DRACOONHttpError
from dracoon.nodes.models import RestoreNode
from dracoon.nodes.responses import DeletedNodeSummaryList

client_id = 'XXXXXXXXXXXXXXXXXXXXXXXX'
client_secret = 'XXXXXXXXXXXXXXXXXXXXXXXX'
base_url = 'https://lts.server.dracoon.systems'
username = 'XXXXXXXXXXXXXXXXXXXXXXXX'
password = 'XXXXXXXXXXXXXXXXXXXXXXXX'

class FolderItem(BaseModel):
    path: str
    level: int
    node_id: Optional[int]
    
    @property
    def name(self):
        return self.path.split('/')[-1]
    
class FolderItemList:
    def __init__(self, folders: List[FolderItem]) -> None:
        self.folders = folders
        
    def get_level(self, level: int) -> List[FolderItem]:
        return [folder for folder in self.folders if folder.level == level]
    
    def get_levels(self) -> List[int]:
        return sorted(set([item.level for item in self.folders]))

async def connect(username: str, password: str) -> DRACOON:
    
    dracoon = DRACOON(client_id=client_id, client_secret=client_secret, log_stream=True, base_url=base_url, log_level=logging.DEBUG)
    try:
        await dracoon.connect(username=username, password=password, connection_type=OAuth2ConnectionType.password_flow, instance_info=False)
    except DRACOONHttpError:
        dracoon.logger.error("Authentication failed.")
        sys.exit(1)        
    return dracoon
    

async def get_deleted_nodes(dracoon: DRACOON, parent_id: int) -> DeletedNodeSummaryList:
    try:
        deleted_nodes = await dracoon.nodes.get_deleted_nodes(parent_id=parent_id)
    except DRACOONHttpError:
        dracoon.logger.error("Retrieving deleted nodes failed.")
        sys.exit(1)    
    
    if deleted_nodes.range.total > 500:
        del_reqs = [asyncio.ensure_future(dracoon.nodes.get_deleted_nodes(parent_id=parent_id, offset=offset)) for offset in range(500, deleted_nodes.range.total, 500)]
        
        for batch in dracoon.batch_process(coro_list=del_reqs, batch_size=3):
            try:
                results: List[DeletedNodeSummaryList] = await asyncio.gather(*batch)
            except DRACOONHttpError:
                for t in del_reqs:
                    t.cancel()
                dracoon.logger.error("Retrieving deleted nodes failed.")
                sys.exit(1)
            else:
                for result in results:
                    deleted_nodes.items.extend(result.items)
    
    return deleted_nodes


def parse_folder_structure(deleted_nodes: DeletedNodeSummaryList) -> FolderItemList:
    # remove trailing '/'
    path_list = set([item.parentPath[:-1] for item in deleted_nodes.items])
    # create item with relevant level
    path_list = [FolderItem(path=path, level=len(path.split('/')) - 1) for path in path_list]
    # create a list to obtain levels
    path_list = FolderItemList(folders=path_list)
    
    return path_list

def get_parent_id(path_list: FolderItemList, parent_path: str) -> int:
    
    return next((folder.node_id for folder in path_list.folders if folder.path.startswith(parent_path)), 0)

async def create_folders(dracoon: DRACOON, path_list: FolderItemList, target_id: int) -> FolderItemList:
    
    for level in path_list.get_levels():
        folders = path_list.get_level(level=level)
        
        for folder in folders:
            # structure created in first level       
            if level == 1:
                parent_id = target_id
            # get parent by matching on parent path
            else:
                parent_id = get_parent_id(path_list=path_list, parent_path=folder.path)
                if parent_id is None: parent_id = target_id
            try:
                payload = dracoon.nodes.make_folder(name=folder.name, parent_id=parent_id)
                new_folder = await dracoon.nodes.create_folder(folder=payload)
                folder.node_id = new_folder.id
            except DRACOONHttpError:
                dracoon.logger.error("Creating folder failed.")
                sys.exit(1)
            
                
async def restore_nodes(dracoon: DRACOON, deleted_nodes: DeletedNodeSummaryList, folder_list: FolderItemList, target_id: int): 
    
    files = [node for node in deleted_nodes.items if node.type == 'file']
    
    for level in folder_list.get_levels():
        folders = folder_list.get_level(level=level)
        
        for folder in folders:
            # get last node id to restore for given path
            lvl_files = [item.lastDeletedNodeId for item in files if item.parentPath[:-1] == folder]
            
            if level == 1:
                parent_id = target_id
            else:
                parent_id = folder.node_id
            
            try:
                payload = dracoon.nodes.make_node_restore(deleted_node_list=lvl_files, parent_id=parent_id)
                await dracoon.nodes.restore_nodes(restore=payload)
            except DRACOONHttpError:
                dracoon.logger.error("Restoring files failed.")
                sys.exit(1)
                
    
async def main():
    
    # log in 
    dracoon = await connect(username=username, password=password)
    # get deleted nodes
    deleted_nodes = await get_deleted_nodes(dracoon=dracoon, parent_id=930)
    # get folder structure
    path_list = parse_folder_structure(deleted_nodes=deleted_nodes)

    # create folder structure in new container
    # cache new folder ids in hashmap 
    path_list = await create_folders(dracoon=dracoon, path_list=path_list, target_id=997)
    
    # restore nodes based on parentPath to id in hashmap
    await restore_nodes(dracoon=dracoon, deleted_nodes=deleted_nodes, folder_list=path_list, target_id=997)
    
if __name__ == '__main__':
    asyncio.run(main())
