from globals import cleaned_files
from sql.FunctionalDependencies import FunctionalDependencies


def main():
    all_fds = {}
    for table_name in cleaned_files:
        fd = FunctionalDependencies(table_name)
        fd.find_functional_dependency()
        all_fds[table_name] = fd.fds  # Store FDs for each table
    FunctionalDependencies.write_all_fds_to_file(all_fds)
    

if __name__ == '__main__':
        main()