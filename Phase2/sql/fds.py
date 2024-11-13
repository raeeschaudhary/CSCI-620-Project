from collections import defaultdict
import ast

from sql.db_utils import connect


def generate_combinations(columnNames, size):
    def recursive_combinations(start, current_combination):
        if len(current_combination) == size:
            result.append(tuple(current_combination))
            return

        for i in range(start, len(columnNames)):
            recursive_combinations(i + 1, current_combination + [columnNames[i]])

    result = []
    recursive_combinations(0, [])
    return result

class FunctionalDependencies:
    def __init__(self,relation):
        self.conn = connect()
        self.table_name = relation
        self.cur = self.conn.cursor()
        self.colnames,self.relation = self.get_data()
        self.partitions = {}
        self.generate_partitions()
        self.fds =  list()
        self.canonical_cover = list()
        self.candidate_keys = list()

    def get_data(self):
        query = (f"SELECT * FROM {self.table_name} LIMIT 10000")
        try:
            self.cur.execute(query)
            rows = self.cur.fetchall()
            colnames = [desc[0] for desc in self.cur.description]
            relation = [dict(zip(colnames, row)) for row in rows]
        except Exception as e:
            print(e)
        finally:
            self.cur.close()
            self.conn.close()
        return colnames, relation

    def _check_dependency(self, lhs, rhs):
        for i in range(len(self.relation)):
            for j in range(i + 1, len(self.relation)):
                lhs_match = all(self.relation[i][attr] == self.relation[j][attr] for attr in lhs)
                if lhs_match and self.relation[i][rhs] != self.relation[j][rhs]:
                    return False
        return True


    def generate_partitions(self):
        for attr in self.colnames:
            part = defaultdict(list)
            for i in range(len(self.relation)):
                row = self.relation[i]
                key = row[attr]
                part[key].append(i)
            # store only the value into the partitions, since we will compare only the value to find FDS
            self.partitions[attr] = list(part.values())

    def refine_partitions(self,lhs_partitions):
        refined_partitions = defaultdict(list)
        for lhs in lhs_partitions:
            for elem in lhs:
                key = tuple(self.relation[elem][attr] for attr in self.colnames)
                #append all the refined partitions into the dictionary with the row as the key,
                refined_partitions[key].append(elem)
        # return only the values, since we dont compare the keys in the partitions, only the appended indices
        return list(refined_partitions.values())

    def _compute_closure_attributes(self, attributes):
        closure = set(attributes)
        while True:
            temp = set(closure)
            for lhs, rhs in self.fds:
                if lhs.issubset(closure):
                    temp.update(rhs)
            if temp == closure:
                break
            closure = temp
        return closure

    def find_functional_dependency(self):
        for lhs_size in range(1,3):
            # The lattice of dependencies represents the subsets of columns.
            lattice = generate_combinations(self.colnames, lhs_size)
            for lhs in lattice:
                lhs_partitions = self.partitions[lhs[0]]
                if len(lhs)>1:
                    lhs_partitions = self.refine_partitions(lhs_partitions)
                for rhs in self.colnames:
                    if rhs not in lhs:
                        rhs_partitions = self.partitions[rhs]
                        # check if every refined LHS partition is a subset of RHS partition thus an FDS
                        if sorted([sorted(partition) for partition in lhs_partitions]) == sorted(rhs_partitions):
                            self.fds.append((set(lhs), {rhs}))
                            #print("Found functional dependency: " + str(lhs)+"-->"+ str(rhs))

    def find_candidate_keys(self):
        all_attributes = set(self.colnames)
        for i in range(1, len(self.colnames) + 1):
            for core in generate_combinations(self.colnames, i):
                if self._compute_closure_attributes(set(core)) == all_attributes:
                    is_superset = False
                    for ck in self.candidate_keys:
                        # Do not consider any attributes which are a superset of an existing candidate key.
                        if set(ck).issubset(core):
                            is_superset = True
                            break
                    if not is_superset:
                        self.candidate_keys.append(core)

    def compute_canonical_cover(self):
        if len(self.fds) == 0:
            self.find_functional_dependency()
        fd_without_extreneuos_attr = []
        for lhs, rhs in self.fds:
            for attr in lhs:
                lhs_without_attr = lhs - {attr}
                # Extreneous Attribute Check
                if self._compute_closure_attributes(lhs_without_attr) == self._compute_closure_attributes(lhs):
                    lhs = lhs_without_attr
            fd_without_extreneuos_attr.append((lhs, rhs))
        canonical_cover_dict = {}
        for lhs, rhs in fd_without_extreneuos_attr:
            str_lhs = str(lhs)
            if str_lhs in canonical_cover_dict:
                canonical_cover_dict[str_lhs].update(rhs)
            else:
                canonical_cover_dict[str_lhs] = rhs
        self.canonical_cover =  [(ast.literal_eval(lhs), rhs) for lhs, rhs in canonical_cover_dict.items()]

    def decompose_to_3nf(self):
        decomposed_relations = []

        for lhs, rhs in self.canonical_cover:
            decomposed_relations.append(lhs.union(rhs))

        for candidate_key in self.candidate_keys:
            if not any(set(candidate_key).issubset(relation) for relation in decomposed_relations):
                decomposed_relations.append(set(candidate_key))
        return decomposed_relations

    @staticmethod
    def write_all_fds_to_file(all_fds, filename="all_tables_fds.txt"):
        with open(filename, "w") as file:
            for table_name, fds in all_fds.items():
                file.write(f"Table: {table_name}\n")
                for lhs, rhs in fds:
                    file.write(f"  {lhs} -> {rhs}\n")
                file.write("\n")
        print(f"All functional dependencies written to {filename}")

if __name__ == '__main__':
    pass