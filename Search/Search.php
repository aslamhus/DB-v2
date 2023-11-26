<?php

namespace LogsheetReader\Search;

use LogsheetReader\Database\DB;
use PDO;

/**
 * Search
 *
 * Summary:
 * - Search a database table for a given search term
 * - Return the results
 *
 *
 * Limitations:
 *
 * - Only supports one table
 * - Requires full text indexes on the columns being searched
 *
 */
class Search
{
    private DB $db;
    private string $table = '';
    private array $select = [];
    private array $order = [];
    private array $columns = [];
    private int $offset = 0;
    private int $limit = 0;
    private array $join = [];

    private array $searchTerms = [];
    private $results = [];
    private string $searchQuery = '';

    /**
     * Constructor
     *
     * @param DB $db
     * @param array $select - the columns to select in the search
     */
    public function __construct(DB $db, array $select = [])
    {

        $this->db = $db;
        $this->select = $select;

    }

    public function search(string $table, array $searchTerms)
    {
        $this->table = $table;
        $this->searchTerms = $searchTerms;
        return $this;

    }

    public function columns(array $columns)
    {
        $this->columns = $columns;
        return $this;
    }

    /**
     * Set the columns to select from the table
     *
     * @param array $select - the columns to select
     */
    public function select(array $select)
    {
        $this->select = $select;
        return $this;
    }

    public function limit(int $offset, int $limit)
    {
        $this->offset = $offset;
        $this->limit = $limit;
        return $this;
    }

    public function order(array $order)
    {
        $this->order = $order;
        return $this;
    }

    public function join(array $join)
    {
        $this->join = $join;
        return $this;
    }

    /**
     * Run the search
     *
     * @example
     * $search = new Search(new DB(['tracks'], ['track','artist','album','label']))
     * ->limit(0,10)->execute();
     *
     * @throws SearchException
     * @return array
     */
    public function execute()
    {

        if(empty($this->table)) {
            throw new SearchException('Could not run search, no table provided');
        }
        if(empty($this->columns)) {
            throw new SearchException('Could not run search, no columns provided');
        }
        if(empty($this->searchTerms)) {
            throw new SearchException('Could not run search, no search terms provided');
        }

        // run search
        $query = null;
        try {
            // match query
            $this->db->matchAgainst($this->table, $this->select, $this->columns, $this->searchTerms)
            ->limit($this->offset, $this->limit)
            ->order($this->order ??  ['relevance DESC'])
            ->join($this->join);
            // execute query
            $this->results = $this->db->execute();


        } catch(\Exception $e) {
            throw new SearchException($e->getMessage(), 0, $e);
        }
        return $this->results;

    }

    /**
     * Get the results of the search
     *
     * @return array
     */
    public function getResults()
    {
        return $this->results;
    }


    /**
     * Get the search query
     *
     * @return string
     */
    public function getSearchQuery()
    {
        return $this->db->getLastQuery();
    }


}


class SearchException extends \Exception
{
    public function __construct($message = '', $code = 0, \Exception $previous = null)
    {
        $message = "Search Exception: " . $message;
        parent::__construct($message, $code, $previous);
    }
}
