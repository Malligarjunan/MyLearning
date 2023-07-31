import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient["local"]
print(mydb.list_collection_names())
mycol = mydb["taxonomies"]


class TreeNode:
    def __init__(self, value):
        self.value = value
        self.children = []

def create_tree_levels(num_levels, num_children_per_node,  parentId):
    if num_levels == 0:
        return

    for i in range(num_children_per_node):
        doc = {}
        id = (parentId*10) + i
        print(id)
        doc['_id'] = str(id)
        doc['name'] = 'term' + str(id)
        doc['parentId'] = str(parentId)

        mycol.insert_one(doc)
        create_tree_levels(num_levels - 1, num_children_per_node,  id)

doc = {}
doc['_id'] = '1'
doc['name'] = 'term1'
doc['parentId'] = 0
mycol.insert_one(doc)

num_levels = 9
num_children_per_node = 3
create_tree_levels(num_levels, num_children_per_node, 1)
