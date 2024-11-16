from collections import defaultdict
import ast

from sql.db_utils import connect
from globals import limit_size


def generate_combinations(columnNames, size):
    """
    Generate all possible combinations of a given size from a list of column names.

    Args:
        columnNames (list): A list of column names.
        size (int): The size of the combinations to generate.

    Returns:
        list: A list of tuples, each containing a combination of column names.
    """
    def recursive_combinations(start, current_combination):
        """
        Helper function to recursively build combinations.

        Args:
            start (int): The starting index for combinations.
            current_combination (list): The current combination being built.
        """
        if len(current_combination) == size:
            result.append(tuple(current_combination))
            return

        for i in range(start, len(columnNames)):
            recursive_combinations(i + 1, current_combination + [columnNames[i]])

    result = []
    recursive_combinations(0, [])
    return result


class FunctionalDependencies:
    """
    A class to discover and manage functional dependencies within a relational dataset.

    Attributes:
        conn (object): Database connection object.
        table_name (str): Name of the table in the database.
        cur (object): Database cursor object.
        colnames (list): List of column names in the table.
        relation (list): Data rows from the table represented as dictionaries.
        partitions (dict): Dictionary of column partitions for dependency checks.
        fds (list): List of discovered functional dependencies.
        canonical_cover (list): Canonical cover of the functional dependencies.
        candidate_keys (list): List of candidate keys in the relation.
    """
    def __init__(self, relation):
        """
        Initialize the FunctionalDependencies class.

        Args:
            relation (str): The name of the database table to analyze.
        """
        self.conn = connect()
        self.table_name = relation
        self.cur = self.conn.cursor()
        self.colnames, self.relation = self.get_data()
        self.partitions = {}
        self.generate_partitions()
        self.fds = []
        self.canonical_cover = []
        self.candidate_keys = []

    def get_data(self):
        """
        Retrieve column names and data from the table.

        Returns:
            tuple: A tuple containing a list of column names and a list of rows as dictionaries.
        """
        # use the limit size from globals.py
        query = f"SELECT * FROM {self.table_name} LIMIT {limit_size}"
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
        """
        Check if a functional dependency holds between the left-hand side (lhs) and right-hand side (rhs).

        Args:
            lhs (list): Attributes on the left-hand side.
            rhs (str): Attribute on the right-hand side.

        Returns:
            bool: True if the dependency holds, False otherwise.
        """
        for i in range(len(self.relation)):
            for j in range(i + 1, len(self.relation)):
                lhs_match = all(self.relation[i][attr] == self.relation[j][attr] for attr in lhs)
                if lhs_match and self.relation[i][rhs] != self.relation[j][rhs]:
                    return False
        return True

    def generate_partitions(self):
        """
        Generate partitions of rows for each attribute in the table.
        """
        for attr in self.colnames:
            part = defaultdict(list)
            for i, row in enumerate(self.relation):
                key = row[attr]
                part[key].append(i)
            self.partitions[attr] = list(part.values())

    def refine_partitions(self, lhs_partitions):
        """
        Refine partitions based on a set of attributes.

        Args:
            lhs_partitions (list): List of partitions for the left-hand side attributes.

        Returns:
            list: Refined partitions.
        """
        refined_partitions = defaultdict(list)
        for lhs in lhs_partitions:
            for elem in lhs:
                key = tuple(self.relation[elem][attr] for attr in self.colnames)
                refined_partitions[key].append(elem)
        return list(refined_partitions.values())

    def _compute_closure_attributes(self, attributes):
        """
        Compute the closure of a set of attributes.

        Args:
            attributes (set): Set of attributes to compute the closure for.

        Returns:
            set: The closure of the attributes.
        """
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
        """
        Discover all functional dependencies in the dataset.
        """
        for lhs_size in range(1, 3):
            lattice = generate_combinations(self.colnames, lhs_size)
            for lhs in lattice:
                lhs_partitions = self.partitions[lhs[0]]
                if len(lhs) > 1:
                    lhs_partitions = self.refine_partitions(lhs_partitions)
                for rhs in self.colnames:
                    if rhs not in lhs:
                        rhs_partitions = self.partitions[rhs]
                        if sorted([sorted(partition) for partition in lhs_partitions]) == sorted(rhs_partitions):
                            self.fds.append((set(lhs), {rhs}))

    def find_candidate_keys(self):
        """
        Identify all candidate keys in the relation.
        """
        all_attributes = set(self.colnames)
        for i in range(1, len(self.colnames) + 1):
            for core in generate_combinations(self.colnames, i):
                if self._compute_closure_attributes(set(core)) == all_attributes:
                    is_superset = False
                    for ck in self.candidate_keys:
                        if set(ck).issubset(core):
                            is_superset = True
                            break
                    if not is_superset:
                        self.candidate_keys.append(core)

    def compute_canonical_cover(self):
        """
        Compute the canonical cover for the discovered functional dependencies.
        """
        if not self.fds:
            self.find_functional_dependency()
        fd_without_extraneous_attr = []
        for lhs, rhs in self.fds:
            for attr in lhs:
                lhs_without_attr = lhs - {attr}
                if self._compute_closure_attributes(lhs_without_attr) == self._compute_closure_attributes(lhs):
                    lhs = lhs_without_attr
            fd_without_extraneous_attr.append((lhs, rhs))
        canonical_cover_dict = {}
        for lhs, rhs in fd_without_extraneous_attr:
            str_lhs = str(lhs)
            if str_lhs in canonical_cover_dict:
                canonical_cover_dict[str_lhs].update(rhs)
            else:
                canonical_cover_dict[str_lhs] = rhs
        self.canonical_cover = [(ast.literal_eval(lhs), rhs) for lhs, rhs in canonical_cover_dict.items()]

    def decompose_to_3nf(self):
        """
        Decompose the relation into 3NF using the canonical cover.

        Returns:
            list: List of decomposed relations as sets of attributes.
        """
        decomposed_relations = []

        for lhs, rhs in self.canonical_cover:
            decomposed_relations.append(lhs.union(rhs))

        for candidate_key in self.candidate_keys:
            if not any(set(candidate_key).issubset(relation) for relation in decomposed_relations):
                decomposed_relations.append(set(candidate_key))
        return decomposed_relations

    @staticmethod
    def write_all_fds_to_file(all_fds, filename="all_tables_fds.txt"):
        """
        Write all functional dependencies for multiple tables to a file.

        Args:
            all_fds (dict): Dictionary of table names and their functional dependencies.
            filename (str): The name of the file to write to. Defaults to "all_tables_fds.txt".
        """
        with open(filename, "w") as file:
            for table_name, fds in all_fds.items():
                file.write(f"Table: {table_name}\n")
                for lhs, rhs in fds:
                    file.write(f"  {lhs} -> {rhs}\n")
                file.write("\n")
        print(f"All functional dependencies written to {filename}")

if __name__ == '__main__':
    pass
