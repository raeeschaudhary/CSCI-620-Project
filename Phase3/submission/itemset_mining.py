from globals import sql_db_config
import psycopg2
import pandas as pd
from collections import defaultdict
from itertools import combinations
import warnings
import time


class KaggleAssociationMiner:
    """
    A class for mining association rules from Kaggle Meta dataset using PostgreSQL database.

    This class implements the Apriori algorithm to discover frequent itemsets and generate
    association rules for various aspects of Kaggle data, including competition tags,
    dataset tags, user achievements, and organization patterns.

    Attributes
    ----------
    conn_params : dict
        Database connection parameters containing dbname, user, password, host, and port

    Methods
    -------
    connect()
        Establishes connection to the PostgreSQL database
    analyze(analysis_type, min_support, min_confidence)
        Performs association rule mining for specified analysis type
    """

    def __init__(self, database, user, password, host, port):
        """
        Initialize the KaggleAssociationMiner with database connection parameters.

        Parameters
        ----------
        database : str
            Name of the database
        user : str
            Database username
        password : str
            Database password
        host : str
            Database host address
        port : str
            Database port number
        """
        self.conn_params = {
            "dbname": database,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }

    def connect(self):
        """
        Create a connection to the PostgreSQL database.

        Returns
        -------
        connection
            psycopg2 connection object
        """
        return psycopg2.connect(**self.conn_params)

    def get_competition_tags_query(self):
        """
        Generate SQL query for competition-tag associations.

        Returns
        -------
        str
            SQL query string for fetching competition tags data
        """
        return """
        SELECT 
            c.id as competition_id,
            c.title as competition_title,
            STRING_AGG(t.name, ',') as tags,
            c.totalteams,
            c.totalcompetitors,
            c.totalsubmissions
        FROM competitionscleaned c
        JOIN competitiontags ct ON c.id = ct.competitionid
        JOIN tags t ON ct.tagid = t.id
        GROUP BY c.id, c.title, c.totalteams, c.totalcompetitors, c.totalsubmissions
        """

    def get_dataset_tags_query(self):
        """
        Generate SQL query for dataset-tag associations.

        Returns
        -------
        str
            SQL query string for fetching dataset tags data
        """
        return """
        SELECT 
            d.id as dataset_id,
            STRING_AGG(t.name, ',') as tags,
            d.totalviews,
            d.totaldownloads,
            d.totalvotes,
            d.totalkernels
        FROM datasetscleaned d
        JOIN datasettags dt ON d.id = dt.datasetid
        JOIN tags t ON dt.tagid = t.id
        GROUP BY d.id, d.totalviews, d.totaldownloads, d.totalvotes, d.totalkernels
        """

    def get_user_organization_query(self):
        """
        Generate SQL query for user organization participation patterns.

        Returns
        -------
        str
            SQL query string for fetching user organization data
        """
        return """
        SELECT 
            u.id as user_id,
            STRING_AGG(o.name, ',') as organizations,
            u.performancetier,
            u.country,
            COUNT(DISTINCT ua.achievementtype) as achievement_types
        FROM users u
        JOIN userorganizations uo ON u.id = uo.userid
        JOIN organizations o ON uo.organizationid = o.id
        LEFT JOIN userachievements ua ON u.id = ua.userid
        GROUP BY u.id, u.performancetier, u.country
        """

    def get_data(self, analysis_type):
        """
        Fetch data from database based on analysis type.

        Parameters
        ----------
        analysis_type : str
            Type of analysis to perform ('competition_tags', 'dataset_tags',or 'user_organizations')

        Returns
        -------
        pandas.DataFrame
            Data fetched from database

        Raises
        ------
        ValueError
            If analysis_type is not recognized
        """
        # Map analysis types to their corresponding query methods
        query_mapping = {
            'competition_tags': self.get_competition_tags_query,
            'dataset_tags': self.get_dataset_tags_query,
            'user_organizations': self.get_user_organization_query
        }

        if analysis_type not in query_mapping:
            raise ValueError(f"Unknown analysis type: {analysis_type}")

        query = query_mapping[analysis_type]()
        # Suppress the Pandas User warnings
        warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
        with self.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def create_transactions(self, df, item_column):
        """
        Convert DataFrame to transaction format for association mining.

        Parameters
        ----------
        df : pandas.DataFrame
            Input DataFrame
        item_column : str
            Name of column containing items to be split into transactions

        Returns
        -------
        list
            List of sets, where each set contains items from one transaction
        """
        transactions = []
        for _, row in df.iterrows():
            items = set(item.strip() for item in row[item_column].split(','))
            transactions.append(items)
        return transactions

    def find_frequent_itemsets(self, transactions, min_support):
        """
        Find frequent itemsets using the Apriori algorithm.

        Parameters
        ----------
        transactions : list
            List of sets, where each set contains items from one transaction
        min_support : float
            Minimum support threshold (0 to 1)

        Returns
        -------
        dict
            Dictionary mapping frequent itemsets to their support values
        """
        # Count individual items first
        total_transactions = len(transactions)
        item_counts = defaultdict(int)

        # Count 1-itemsets
        for transaction in transactions:
            for item in transaction:
                item_counts[frozenset([item])] += 1

        min_count = min_support * total_transactions

        # Initialize with frequent 1-itemsets
        frequent_itemsets = {
            itemset: count / total_transactions
            for itemset, count in item_counts.items()
            if count >= min_count
        }

        # Iteratively find frequent itemsets of increasing size
        k = 2
        while True:
            # Generate candidate itemsets
            candidates = set()
            current_frequent = set(frequent_itemsets.keys())

            for set1 in current_frequent:
                for set2 in current_frequent:
                    union = set1.union(set2)
                    if len(union) == k:
                        candidates.add(union)

            if not candidates:
                break

            # Count candidate occurrences
            candidate_counts = defaultdict(int)
            for transaction in transactions:
                for candidate in candidates:
                    if candidate.issubset(transaction):
                        candidate_counts[candidate] += 1

            # Add new frequent itemsets
            new_frequent = {
                itemset: count / total_transactions
                for itemset, count in candidate_counts.items()
                if count >= min_count
            }

            if not new_frequent:
                break

            frequent_itemsets.update(new_frequent)
            k += 1

        return frequent_itemsets

    def generate_rules(self, frequent_itemsets, min_confidence):
        """
        Generate association rules from frequent itemsets.

        Parameters
        ----------
        frequent_itemsets : dict
            Dictionary mapping frequent itemsets to their support values
        min_confidence : float
            Minimum confidence threshold (0 to 1)

        Returns
        -------
        list
            List of dictionaries containing association rules and their metrics
        """
        rules = []

        for itemset, support in frequent_itemsets.items():
            if len(itemset) < 2:
                continue

            # Generate all possible antecedent/consequent splits
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = frozenset(antecedent)
                    consequent = itemset - antecedent

                    if antecedent not in frequent_itemsets:
                        continue

                    # Calculate confidence
                    confidence = support / frequent_itemsets[antecedent]

                    if confidence >= min_confidence:
                        if consequent not in frequent_itemsets:
                            continue

                        # Calculate lift
                        lift = confidence / frequent_itemsets[consequent]

                        rules.append({
                            'antecedent': set(antecedent),
                            'consequent': set(consequent),
                            'support': support,
                            'confidence': confidence,
                            'lift': lift
                        })

        return rules

    def analyze(self, analysis_type, min_support=0.01, min_confidence=0.5):
        """
        Perform complete association rule mining analysis.

        Parameters
        ----------
        analysis_type : str
            Type of analysis to perform ('competition_tags', 'dataset_tags',
            or 'user_organizations')
        min_support : float, optional (default=0.01)
            Minimum support threshold
        min_confidence : float, optional (default=0.5)
            Minimum confidence threshold

        Returns
        -------
        tuple
            (frequent_itemsets, rules, df) containing the discovered patterns
            and original data
        """
        # Get data for analysis
        df = self.get_data(analysis_type)

        # Map analysis types to their item columns
        item_column_mapping = {
            'competition_tags': 'tags',
            'dataset_tags': 'tags',
            'user_organizations': 'organizations'
        }

        item_column = item_column_mapping[analysis_type]

        # Create transactions and find patterns
        transactions = self.create_transactions(df, item_column)
        frequent_itemsets = self.find_frequent_itemsets(transactions, min_support)
        rules = self.generate_rules(frequent_itemsets, min_confidence)

        return frequent_itemsets, rules, df


def print_analysis_results(analysis_type, frequent_itemsets, rules, df, top_n=10):
    """Print formatted analysis results with context-specific metrics"""
    
    print('\n ++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"\n=== {analysis_type.replace('_', ' ').title()} Analysis ===")

    # Print basic statistics
    print("\nDataset Statistics:")
    print(f"Total records analyzed: {len(df)}")

    # Print frequent itemsets
    print(f"\nTop {top_n} Frequent Item Combinations:")
    frequent_items_df = pd.DataFrame([
        {'items': ' + '.join(sorted(itemset)), 'support': support}
        for itemset, support in frequent_itemsets.items()
    ]).sort_values('support', ascending=False).head(top_n)
    print(frequent_items_df)

    # Print rules
    print(f"\nTop {top_n} Association Rules:")
    rules_df = pd.DataFrame([
        {
            'if': ' + '.join(sorted(rule['antecedent'])),
            'then': ' + '.join(sorted(rule['consequent'])),
            'support': rule['support'],
            'confidence': rule['confidence'],
            'lift': rule['lift']
        }
        for rule in rules
    ]).sort_values('confidence', ascending=False).head(top_n)
    print(rules_df)

    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Print analysis-specific metrics
    if analysis_type == 'competition_tags':
        print("\nCompetition Statistics:")
        print(f"Average teams per competition: {df['totalteams'].mean():.2f}")
        print(f"Average competitors per competition: {df['totalcompetitors'].mean():.2f}")
    elif analysis_type == 'dataset_tags':
        print("\nDataset Statistics:")
        print(f"Average views per dataset: {df['totalviews'].mean():.2f}")
        print(f"Average downloads per dataset: {df['totaldownloads'].mean():.2f}")
    
    print('++++++++++++++++++++++++++++++++++++++++++++++ \n')


if __name__ == "__main__":
    start_time = time.time()

    db_params = sql_db_config
    miner = KaggleAssociationMiner(**db_params)

    # Analyze 'competition_tags', 'dataset_tags', 'user_organizations'
    analysis_type = 'competition_tags'
    frequent_itemsets, rules, df = miner.analyze(
        analysis_type=analysis_type,
        min_support=0.01,
        min_confidence=0.5
    )

    print_analysis_results(analysis_type, frequent_itemsets, rules, df)

    analysis_type = 'dataset_tags'
    frequent_itemsets, rules, df = miner.analyze(
        analysis_type=analysis_type,
        min_support=0.005,
        min_confidence=0.4
    )

    print_analysis_results(analysis_type, frequent_itemsets, rules, df)

    analysis_type = 'user_organizations'
    frequent_itemsets, rules, df = miner.analyze(
        analysis_type=analysis_type,
        min_support=0.01,
        min_confidence=0.4
    )

    print_analysis_results(analysis_type, frequent_itemsets, rules, df)


    print('++++++++++++++++++++++++++++++++++++++++++++++')
    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")