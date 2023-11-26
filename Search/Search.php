<?php

namespace LogsheetReader\Search;

use LogsheetReader\Database\DB;

/**
 * Search
 *
 *
 * Summary:
 * - Search a database table for a given search term
 * - Return the results
 *
 * @example
 *
 *  $search = new Search($db);
 *  $search->search('tracks', $params['search'])
 *  ->columns($columnsToSearch)
 *  ->select($columnsToSelect)
 *  ->limit(0, 100)
 *  ->join(['LEFT JOIN shows ON tracks.show_id = shows.id'])
 *  ->execute();
 *
 *  $results = $search->getResults();
 *
 *
 * Requirements:
 *
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

    /**
     * Set the search table and terms
     *
     * @param string $table
     * @param array $searchTerms
     * @return self
     */
    public function search(string $table, array $searchTerms): self
    {
        $this->table = $table;
        $this->searchTerms = $searchTerms;
        return $this;

    }

    /**
     * Set the columns to search
     *
     * @param array $columns
     * @return self
     */
    public function columns(array $columns): self
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

    /**
     * Set the limit and offset
     *
     * @param integer $offset
     * @param integer $limit
     * @return self
     */
    public function limit(int $offset, int $limit): self
    {
        $this->offset = $offset;
        $this->limit = $limit;
        return $this;
    }

    /**
     * Set the order
     *
     * @param array $order
     * @return self
     */
    public function order(array $order): self
    {
        $this->order = $order;
        return $this;
    }

    /**
     * Set the join
     *
     * @example join(['LEFT JOIN shows ON tracks.show_id = shows.id'])
     *
     * @param array $join
     * @return self
     */
    public function join(array $join): self
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
    public function execute(): array
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
        if(empty($this->select)) {
            throw new SearchException('Could not run search, no select columns provided');
        }
        // run the search
        try {
            $startTime = microtime(true);
            // for each column, perform a match against the given search value
            $index = 0;
            foreach($this->columns as $column) {
                $this->db->matchAgainst($this->table, $this->select, [$column], $this->searchTerms)
                ->limit($this->offset, $this->limit)
                ->order($this->order ??  ['relevance DESC'])
                ->join($this->join);
                // execute query
                $this->results['results'][$index]['column'] = $column;
                $this->results['results'][$index]['items'] = $this->db->execute();
                $this->results['results'][$index]['query'] = $this->db->getLastQuery();
                $this->results['results'][$index]['performance'] = $this->db->getPerformance();
                $this->results['results'][$index]['resultTotal'] = $this->db->getRowCount();
                // get total entries
                $totalEntries = $this->getTotalEntires($column);
                // build pagination details
                if($totalEntries > 0) {
                    $this->results['results'][$index]['pagination'] = $this->createPaginationDetails($totalEntries, $this->limit, $this->offset);
                }
                // get the search query
                $this->searchQuery = $this->db->getLastQuery();
                // get the search performance
                $performance[$column] = $this->db->getPerformance();
                $index++;
            }

            // sort the rsults by total rows
            $this->results['results'] = $this->sortResultsByTotalRows($this->results['results']);
            // return search performance time
            $this->results['performance'] = microtime(true) - $startTime;
            $this->results['columns']= $this->columns;


        } catch(\Exception $e) {
            throw new SearchException($e->getMessage(), 0, $e);
        }
        return $this->results;

    }

    /**
     * Sort the results by total rows
     *
     * @param array $results
     * @return array $results
     */
    private function sortResultsByTotalRows(array $results): array
    {
        usort($results, function ($a, $b) {
            $aResults = $a['pagination']['totalEntries'] ?? $a['resultTotal'];
            $bResults = $b['pagination']['totalEntries'] ?? $b['resultTotal'];
            return $bResults <=> $aResults;
        });

        return $results;
    }

    /**
     * Get total entries
     *
     * perform the search query without the limit to get the total results
     *
     * @param string $column
     * @return int
     */
    private function getTotalEntires(string $column): int
    {
        $totalEntries = 0;
        // allow COUNT(*) column
        $this->db->setAllowList([], ['COUNT(*) as count']);
        // for this query, we don't want to include relevance
        $includeRelevance = false;
        // get the total entries
        $this->db->matchAgainst($this->table, ['COUNT(*) as count'], [$column], $this->searchTerms, $includeRelevance)
        ->order(['count DESC']);
        $totalEntries = $this->db->execute()[0]['count'] ?? 0;

        return $totalEntries;
    }

    /**
     * Create pagination details
     *
     * @param integer $totalEntries
     * @param integer $limit
     * @param integer $offset
     * @return array $pagination
     */
    private function createPaginationDetails(int $totalEntries, int $limit, int $offset): array
    {
        $pagination = [];
        $pagination['totalEntries'] = $totalEntries;
        $pagination['totalPages'] = $this->calcTotalPages($totalEntries, $limit);
        $pagination['currentPage'] = $this->calcCurrentPage($offset, $limit);
        $pagination['limit'] = $limit;
        $pagination['offset'] = $offset;
        return $pagination;
    }

    /**
     * Calc total pages
     *
     * @param integer $totalEntries
     * @param integer $limit
     * @return int
     */
    private function calcTotalPages(int $totalEntries, int $limit): int
    {
        return ceil($totalEntries / $limit);
    }

    /**
     * Calc current page
     *
     * @param integer $offset
     * @param integer $limit
     * @return int
     */
    private function calcCurrentPage(int $offset, int $limit): int
    {
        return ceil($offset / $limit) + 1;
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
