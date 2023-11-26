<?php

namespace LogsheetReader\Database;

use PDO;
use PDOStatement;

/**
 * QueryBuilder
 * v1.0.0
 *
 * by @aslamhus
 *
 * To be used in conjunction with LogsheetsReader\Database\DB
 *
 * @example
 * $query = new QueryBuilder($pdo);
 * $query->select(['col1','col2'])
 *      ->from('mytable')
 *      ->where('col','=','myvalue')
 *     ->and('col2','<','othervalue')
 *    ->or('col3','>','yetanothervalue')
 *  ->group(['col1','col2'])
 * ->order('col1')
 * ->limit(0,10)
 * ->get();
 *
 *
 *
 *
 *
 *
 */

class QueryBuilder
{
    private PDO $pdo;
    private PDOStatement $stmt;
    private string $query = '';
    private string $table = '';
    private array $columns = [];
    private array $where = [];
    private array $order = [];
    private array $group = [];
    private array $join = [];
    private int $offset = 0;
    private int $limit = 0;
    private array $bindValues = [];
    private array $matches = [];

    public function __construct(\PDO $pdo)
    {
        $this->pdo = $pdo;

    }

    /**
     * Select
     *
     * @param array $columns
     * @return QueryBuilder
     */
    public function select(array $columns): QueryBuilder
    {
        $this->columns = array_merge($this->columns, $columns);

        return $this;
    }

    /**
     * From
     *
     * @param string $table
     * @return QueryBuilder
     */
    public function from(string $table): QueryBuilder
    {
        $this->table = $table;

        return $this;
    }


    /**
     * Where
     *
     * Example:
     *
     * first where: where('col','=','myvalue'), translates into WHERE col = 'myvalue'
     * subsequent wheres require logic gate: where('col','<','othervalue','AND'), translates into AND col < 'othervalue'
     *
     *
     * @param string $column - the column
     * @param string $operator - <,=,<=,>=,<>
     * @param string $value - the value
     * @param string $logicGate - AND | OR | WHERE
     * @param string [$searchModifier] - IN BOOLEAN MODE | IN NATURAL LANGUAGE MODE | WITH QUERY EXPANSION | IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION
     *
     * @return QueryBuilder
     */
    public function where(string $column, string $operator, string $value, string $logicGate = '', string $searchModifier = ''): QueryBuilder
    {
        // first where clause
        $first = empty($this->where);
        if(!$first && empty($logicGate)) {
            throw new QueryException("Please specify a logic gate for subsequent where clauses");
        }
        if($first) {
            $logicGate = 'WHERE';
        }
        $this->where[] = [$column, $operator, $value, $logicGate, $searchModifier];


        return $this;
    }

    public function and(string $column, string $operator, string $value): QueryBuilder
    {
        $this->where($column, $operator, $value, 'AND');
        return $this;
    }

    public function or(string $column, string $operator, string $value): QueryBuilder
    {
        $this->where($column, $operator, $value, 'OR');
        return $this;
    }

    /**
     * Match
     * Adds a match clause to the where clause
     *
     * TODO: set option for partial or precise match (including * wildcards)
     *
     * @param array $columns
     * @param string $value
     * @param string $searchModifier
     * @param string $logicGate
     * @param bool $includeRelevance - whether to include the relevance in the select and order by relevance
     * @return QueryBuilder
     */
    public function match(array  $columns, string $value, string $searchModifier, string $logicGate = '', bool $includeRelevance = true): QueryBuilder
    {

        // check if this is the first where clause
        $first = empty($this->where);
        // default to AND operator
        if(!$first && empty($logicGate)) {
            $logicGate = 'AND';
        }
        // validate search modifier
        if(!in_array($searchModifier, ['IN BOOLEAN MODE', 'IN NATURAL LANGUAGE MODE', 'WITH QUERY EXPANSION', 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION'])) {
            throw new QueryException("Invalid search modifier '".$searchModifier."'");
        }
        // add partial or precise match
        $value = "*".$value."*";
        // add the match to the where clause
        $this->where('MATCH ('.implode(',', $columns).')', 'AGAINST', $value, $logicGate, $searchModifier);
        // add the match in the select to get the relevance
        $this->matches[]  = ['MATCH ('.implode(',', $columns).') AGAINST (? '.$searchModifier.') as relevance', $value, $includeRelevance];


        return $this;
    }

    /**
     * Select match scores
     * Adds the match scores to the select
     *
     * @return array
     */
    private function selectMatchScores(): array
    {
        // add the match scores to the select
        foreach($this->matches as $index => $match) {
            list($select, $value, $includeRelevance) = $match;
            if(!$includeRelevance) {
                continue;
            }
            // add the match against to the select
            $select = str_replace('relevance', "`relevance$index`", $select);
            $this->select([$select]);
            $this->pushBindValue($value);
            // add relevance to the select
            $this->select(["? as `relevance".$index."Term`"]);
            $this->pushBindValue($value);

        }
        // add the sum of the relevance columns to the order by
        if($includeRelevance) {
            $order = "";
            $index = 0;
            foreach($this->matches as $index => $match) {
                $order .= "`relevance$index`";
                if($index < count($this->matches) - 1) {
                    $order .= "+";
                }
                $index++;
            }
            $order .= " DESC";
            // add sum of relevance columns
            $this->order([$order]);
        }


        return $this->matches;
    }



    /**
     * Group
     *
     * @param string $groupColumns - the columns to group by
     * @return QueryBuilder
     */
    public function group(array $groupColumns): QueryBuilder
    {
        $this->group = array_merge($groupColumns, $this->group);
        return $this;
    }



    /**
     * Order
     *
     * @param array $order
     * @return QueryBuilder
     */
    public function order(array $order): QueryBuilder
    {
        $this->order = array_merge($this->order, $order);
        return $this;
    }

    /**
     * Join
     *
     * @param array $join - i.e. ['JOIN table on table1.field = table2.field', 'LEFT JOIN table on table1.field = table2.field']
     * @return QueryBuilder
     */
    public function join(array $join): QueryBuilder
    {
        $this->join = array_merge($this->join, $join);
        return $this;
    }

    /**
     * Limit
     *
     * @param integer $offset - the offset where the entries should start
     * @param integer $limit - the number of entries to return
     * @return QueryBuilder
     */
    public function limit(int $offset, int $limit): QueryBuilder
    {
        $this->offset = $offset;
        $this->limit = $limit;

        return $this;
    }


    /**
     * Execute the query
     *
     * @return PDOStatement
     */
    public function execute(): PDOStatement
    {
        // validate that the minimum requirements to build a query are satisfied
        if(empty($this->table)) {
            throw new QueryException("Table not defined");
        }
        if(empty($this->columns)) {
            throw new QueryException("Columns to query not defined");
        }
        if(empty($this->where)) {
            throw new QueryException('Where clause empty');

        }
        // build the query
        $this->query = $this->buildQuery();
        // prepare the pdo query
        $this->stmt =$this->pdo->prepare($this->query);
        // bind the values
        foreach($this->bindValues as $index => $value) {
            $this->stmt->bindValue($index + 1, $value);
        }
        // execute the statement
        $this->stmt->execute();

        return $this->stmt;
    }

    public function getRowCount(): int
    {
        if(empty($this->stmt)) {
            throw new QueryException("Failed to get row count. No query was performed.");
        }
        return $this->stmt->rowCount();
    }


    private function buildQuery(): string
    {
        // before we build the query, check if we need to add the matches to the select
        if(!empty($this->matches)) {
            $this->selectMatchScores();
        }
        // Select col1,col2
        $this->query = 'SELECT '.implode(',', $this->columns);
        // from table
        $this->query  .= " from {$this->table}";
        // jointype tablename ie. LEFT JOIN mytable
        if(!empty($this->join)) {
            $this->query  .= $this->implodeArgumentsForQuery($this->join);
        }
        // where col operand ?, col operand ?...
        $this->query  .= $this->buildWhereLogic();

        // group by col1,col2,col3...
        if(!empty($this->group)) {
            $this->query .= ' GROUP BY ' .$this->implodeArgumentsForQuery($this->group);
        }
        // order by col1,col2,col3...
        if(!empty($this->order)) {
            $this->query  .= ' ORDER BY '. $this->implodeArgumentsForQuery($this->order);
        }
        // limit offset, entriesToReturn
        if(!empty($this->limit)) {
            $this->query  .= " LIMIT {$this->offset}, {$this->limit}";
        }


        return $this->query;

    }



    /**
     * Implode arguments for query
     *
     * A buildQquery helper
     *
     * @param array $args
     * @return string
     */
    private function implodeArgumentsForQuery(array $args): string
    {
        return " ".implode(',', $args);

    }

    /**
     * Insert where logic argument
     *
     * A buildWhereLogic helper
     *
     * @param string $column
     * @param string $operator
     * @param string $value
     * @param string $logicGate
     * @param string [$searchModifier]
     *
     * @return string
     */
    private function insertWhereLogicArgument(string $column, string $operator, string $value, string $logicGate, string $searchModifier =''): string
    {
        $argument = '';
        $allowedLogicGates = [
            'AND',
            'OR',
            'WHERE',
        ];
        if(!in_array($logicGate, $allowedLogicGates)) {
            throw new QueryException("Invalid logic gate '".$logicGate."'");
        }
        // get the parts of the expression

        $argument .= " $logicGate $column $operator ?";
        // if the argument contains a match, the value needs to be wrapped
        // in parenthesis with an optional search modifier i.e. (? IN BOOLEAN MODE)
        // as well as optional wildcards i.e. *value*
        if(str_contains($argument, 'MATCH')) {
            $placeholder = '(?';
            if(!empty($searchModifier)) {
                $placeholder .= ' '.$searchModifier;
            }
            $placeholder .= ')';
            $argument = preg_replace('/\?/', $placeholder, $argument, 1);
        }
        // push to bindValue
        $this->pushBindValue($value);
        return $argument;

    }

    /**
     * Build the where logic
     *
     * @return string
     */
    private function buildWhereLogic(): string
    {
        $where = '';
        // AND logic
        foreach($this->where as $whereExpression) {
            $where .= $this->insertWhereLogicArgument(...$whereExpression);
        }

        return $where;

    }

    /**
     * Push a value to be bound (replacing question marks in prepared statement)
     *
     * @param string $value
     * @return void
     */
    private function pushBindValue(string $value)
    {
        $this->bindValues[] = $value;
    }

    public function getLastQuery(): string
    {
        return $this->query;
    }

    /**
    * Get the query string with bindValues filled in
    *
    * @param bool $withParameters - whether to include the parameters
    * @return string
    */
    public function toString(bool $withParameters  = true): string
    {
        if(empty($this->query)) {
            return '';
        }
        $queryString =  $this->query;
        if(!$withParameters) {
            return $queryString;
        }
        foreach($this->bindValues as $index => $value) {
            $queryString = preg_replace("/\?/", "'".$value."'", $queryString, 1, $index);
        }
        return $queryString;
    }

    /**
     * Get the performance of the query
     *
     *
     *
     * @return array - the performance of the query in number of page reads
     */
    public function getPerformance(): array
    {
        return $this->pdo->query('SHOW STATUS LIKE "Last_Query_Cost"')->fetch();
    }
}


class QueryException extends \Exception
{
    public function __construct(string $message, $code = 0)
    {
        parent::__construct($message, $code, null);

    }
}
