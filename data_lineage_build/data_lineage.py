"""We have data table consisting of columns source_vdb,source_viewname,source_column_name 
and target_vdb,target_viewname ,target_column_name
We need to find the complete data lineage information
We are keeping in mind that the circular dependency between the source and target will not exists
example:
List_of_tuple_of_source_target_pair = [(s1,d1),(d1,d2),(d2,d3),(d3,s2),(s3,t1),(t1,p2),(p2,s4)]
We can extract the this data from the data table.
we want to result as 
Complete Paths:
s1 -> d1 -> d2 -> d3 -> s2
s3 -> t1 -> p2 -> s4
"""

from collections import defaultdict
import pandas as pd

# Input list of tuples
edges = [("s1", "d1"), ("d1", "d2"), ("d2", "d3"), ("d3", "s2"),
         ("s3", "t1"), ("t1", "p2"), ("p2", "s4"),
         ("d2","d9"),("d9","d10")]

# graph creation
graph = defaultdict(list)
all_destinations = set()

for src, dest in edges:
    graph[src].append(dest)
    all_destinations.add(dest)
print('garph : ',graph)
print('all_destination: ',all_destinations)
# Identify true source nodes (nodes that are not destinations)
true_sources = [node for node in graph if node not in all_destinations]
print('true_sources: ',true_sources)

# Find all complete paths using DFS 
def dfs(node, path, visited_paths, visited_set):
    # Add the current node to the path and mark it as visited in the current path
    path.append(node)
    visited_set.add(node)
    print(f'path: {path}')

    # If the node has no outgoing edges, we've reached a complete path
    if node not in graph or not graph[node]:
        visited_paths.append(path[:])
        print(f'visited_paths: {visited_paths}')
    else:
        # Traverse each neighbor
        for neighbor in graph[node]:
            if neighbor not in visited_set:  # Avoid revisiting nodes in the current path
                dfs(neighbor, path, visited_paths, visited_set)
    
    # Backtrack: Remove the current node from path and visited set
    path.pop()
    print('pop path:',path)
    visited_set.remove(node)

# Find all paths starting from each true source node
complete_paths = []
for src in true_sources:
    dfs(src, [], complete_paths, set())
# print('complete_paths: ',complete_paths)
# Print all complete paths
print("Complete Paths:")
for path in complete_paths:
    print(" -> ".join(path))

######################################

## Changes as per project requirement

class lineage():
    def __intit__(self,sf_format,sfOptions):
        self.sf_format = sf_format
        self.sfOptions=sfOptions
    def fetch_lineage_data(self,table_name):
        df = spark.read.format(self.sf_format).options(*self.sfOptions).option("dbtable", table_name).load()
        df = df.select('source_vdb','source_viewname','source_column_name','target_vdb','target_viewname' ,'target_column_name')
        df = df.select(F.concat_ws('.',F.col('source_vdb'),F.col('source_viewname'),F.col('source_column_name')).alias('source'),
                       F.concat_ws('.',F.col('target_vdb'),F.col('target_viewname'),F.col('target_column_name')).alias('target'))
        
        list_of_tuple = [(row['source'], row['target']) for row in df.collect()]
        return list_of_tuple
    
    def dfs(self,node, path, visited_paths, visited_set):
        # Add the current node to the path and mark it as visited in the current path
        path.append(node)
        visited_set.add(node)

        # If the node has no outgoing edges, we've reached a complete path
        if node not in graph or not graph[node]:
            visited_paths.append(path[:])
        else:
            # Traverse each neighbor
            for neighbor in graph[node]:
                if neighbor not in visited_set:  # Avoid revisiting nodes in the current path
                    dfs(neighbor, path, visited_paths, visited_set)
        
        # Backtrack: Remove the current node from path and visited set
        path.pop()
        visited_set.remove(node)

    def detect_complete_path(self,list_of_tuple):
        from collections import defaultdict
        graph = defaultdict()
        all_destinations = set()

        for src, dest in list_of_tuple:
            graph[src].append(dest)
            all_destinations.add(dest)
        
        true_sources = [node for node in graph if node not in all_destinations]

        complete_paths = []
        for src in true_sources:
            self.dfs(src, [], complete_paths, set())
        
        start_vertices = []
        end_vertices = []
        internal_vertices_list = []
        for path in complete_paths:
            start_vertices.append(path[0])           # First element
            end_vertices.append(path[-1])           # Last element
            internal_vertices_list.append(path[1:-1])  # Middle elements
        
        # Dynamically create columns for internal vertices
        max_internal_vertices = max(len(internal) for internal in internal_vertices_list)
        internal_columns = [f'internal_vertices_{i+1}' for i in range(max_internal_vertices)]

        rows = []
        for start, end, internals in zip(start_vertices, end_vertices, internal_vertices_list):
            row = [start, end] + internals + [None] * (max_internal_vertices - len(internals))
            rows.append(row)

        # Create the DataFrame
        columns = ['start_vertices', 'end_vertices'] + internal_columns
        df = pd.DataFrame(rows, columns=columns)

        spark_df = spark.createDataFrame(df)
        spark_df = spark_df.select('start_vertices',*internal_columns,'end_vertices')

        spark_df.write.format(self.sf_format).format(*self.sfOptions).option('mode','overwrite').option("dbtable", 'lineage_data').save()






        






