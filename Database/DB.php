<?php

namespace LogsheetReader\Database;

use LogsheetReader\Database\QueryBuilder;
use PDO;

/**
 * DB Class
 *
 * v1.0.0
 *
 * by @aslamhus
 *
 * To be used in conjunction with LogsheetsReader\Database\QueryBuilder
 *
 * @example
 *  $db = new DB();
 * $db->searchLike('tracks', ['track','artist','album','label'], ['Miles','Davis']);
 * $results = $db->execute();
 */
class DB
{
    public PDO $pdo;
    private QueryBuilder $query;
    private array $columnAllowList = [];
    private array $tableAllowList = [];

    /**
     * Constructor
     *
     * @param array $columnAllowList - list of columns to allow in database queries
     * @param array $tableAllowList - list of tables to allow in database queries
     */
    public function __construct(array $tableAllowList = [], array $columnAllowList = [])
    {
        // set the allow lists
        $this->tableAllowList = $tableAllowList;
        $this->columnAllowList = $columnAllowList;
        // connect to the database
        $this->connect();

    }

    /**
     * Connect to the database
     *
     * @return void
     */
    private function connect()
    {

        // init the connection options
        $connect = [
            'host' => $_ENV['DB_HOST'],
            'db'   => $_ENV['DB_NAME'],
            'user' => $_ENV['DB_USER'],
            'pass' => $_ENV['DB_PASS']
        ];
        $dsn = "mysql:host=".$connect['host'].";dbname=".$connect['db'].";charset=utf8mb4";
        $options = [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES   => false,
        ];
        // connect to the database
        try {
            $this->pdo = new PDO($dsn, $connect['user'], $connect['pass'], $options);
        } catch (\PDOException $e) {
            throw new \PDOException("DB Connect error: ".$e->getMessage(), (int)$e->getCode());
        }
    }

    /**
     * Set the allow lists
     *
     * @param array $tableAllowList - list of tables to allow in database queries
     * @param array $columnAllowList - list of columns to allow in database queries
     * @return void
     */
    public function setAllowList(array $tableAllowList, array $columnAllowList)
    {
        $this->tableAllowList = $tableAllowList;
        $this->columnAllowList = $columnAllowList;
    }

    public function getPDO(): PDO
    {
        return $this->pdo;
    }

    /**
     * Search the database for a given table and parameters
     * use the like operator
     *
     * @param string $table
     * @param array $columns - columns to search
     * @param array $searchTerm - the search terms
     *
     * @return QueryBuilder
     */
    public function searchLikeQuery(string $table, array $columns, array $searchTerms)
    {
        // validate the table
        $this->isTableValid($table);
        // validate the columns
        foreach($columns as $column) {
            $this->isColumnValid($column);
        }
        // validate the search term
        if(empty($searchTerms)) {
            throw new \Exception('Failed to perform database query - no search parameters provided');
        }
        // build the query
        $this->query = new QueryBuilder($this->pdo);
        $this->query->select($columns)->from($table);
        $index = 0;
        foreach($columns as $col) {
            foreach($searchTerms as $term) {
                $query =  $this->query->where($col, 'LIKE', "%".$term."%");
                $index++;
            }
        }


        return  $this->query;

    }


    /**
     * Match the database for a given table and parameters
     *
     * @param string $table
     * @param array $select - columns to select
     * @param array $columns - columns to search
     * @param array $searchTerm - the search terms
     *
     * @throws \Exception
     * @return QueryBuilder
     */
    public function matchAgainst(string $table, array $select, array $columns, array $searchTerms): QueryBuilder
    {

        $stmt = new \PDOStatement();
        // validate the table
        $this->isTableValid($table);
        // validate the columns to select
        foreach($select as $column) {
            $this->isColumnValid($column);
        }
        // validate the  columns to search
        foreach($columns as $column) {
            $this->isColumnValid($column);
        }

        // validate the search term
        if(empty($searchTerms)) {
            throw new \Exception('Failed to perform database query - no search parameters provided');
        }
        // build the query
        $this->query = new QueryBuilder($this->pdo);
        // select the columns from the table
        $this->query = $this->query->select($select)->from($table);
        // match the columns with the search terms
        foreach($searchTerms as $term) {
            $this->query->match($columns, $term, 'IN BOOLEAN MODE', 'OR');
        }
        // return the query object
        return $this->query;

    }

    /**
     * Execute a query
     *
     * @param string $query - the query to execute
     *
     * @return array
     */
    public function execute()
    {
        // validate the query
        if(empty($this->query)) {
            throw new \Exception('Failed to perform database query - no query set');
        }
        // execute the query
        try {
            $stmt = $this->query->execute();
        } catch(\Exception $e) {
            throw new \Exception('Failed to perform database query: '.$e->getMessage());
        }

        // return the results
        return $stmt->fetchAll();
    }

    private function isTableValid(string $table): bool
    {
        if($this->tableAllowList === ['*'] || empty($this->tableAllowList)) {
            return true;
        }
        if(!in_array($table, $this->tableAllowList)) {
            throw new \Exception('Table not allowed');
        }
        return true;
    }

    private function isColumnValid(string $column): bool
    {
        if($this->columnAllowList === ['*'] || empty($this->columnAllowList)) {
            return true;
        }
        if(!in_array($column, $this->columnAllowList)) {
            throw new \Exception('Column not allowed: '.$column.'');
        }
        return true;
    }

    public function getLastQuery(): string
    {
        if(empty($this->query)) {
            throw new \Exception('No query set');
        }
        return $this->query->getLastQuery();
    }




}
