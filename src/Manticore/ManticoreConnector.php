<?php

namespace Core\Manticore;

use Core\Logger\Logger;
use mysqli;

class ManticoreConnector
{
    public const INDEX_TYPE_PERCOLATE = 'percolate';
    public const INDEX_TYPE_RT = 'rt';
    protected int $maxAttempts;
    protected mysqli $connection;
    protected string $clusterName;
    protected string $rtInclude;
    protected $fields;
    protected array $searchdStatus = [];

    public function __construct($host, $port, $label, $maxAttempts)
    {
        $this->setMaxAttempts($maxAttempts);
        $this->clusterName = $label.'_cluster';

        for ($i = 0; $i <= $this->maxAttempts; $i++) {
            $this->connection = new mysqli($host.':'.$port, '', '', '');

            if ( ! $this->connection->connect_errno) {
                break;
            }

            sleep(1);
        }

        if ($this->connection->connect_errno) {
            throw new \RuntimeException("Can't connect to Manticore at ".$host.':'.$port);
        }
    }

    public function setCustomClusterName($name)
    {
        $this->clusterName = $name;
    }

    public function setMaxAttempts($maxAttempts): void
    {
        if ($maxAttempts === -1) {
            $maxAttempts = 999999999;
        }
        $this->maxAttempts = $maxAttempts;
    }

    public function getStatus($log = true): void
    {
        $clusterStatus = $this->fetch("show status", $log);

        foreach ($clusterStatus as $row) {
            $this->searchdStatus[$row['Counter']] = $row['Value'];
        }
    }

    public function getTables($log = true): array
    {
        $tables     = [];
        $tablesStmt = $this->fetch("show tables", $log);

        foreach ($tablesStmt as $row) {
            $tables[] = $row['Index'];
        }

        return $tables;
    }

    public function isTableExist($tableName): bool
    {
        $tables = $this->getTables();

        return in_array($tableName, $tables);
    }

    public function checkClusterName(): bool
    {
        if ($this->searchdStatus === []) {
            $this->getStatus();
        }

        return (isset($this->searchdStatus['cluster_name'])
                && $this->searchdStatus['cluster_name'] === $this->clusterName) ?? false;
    }

    /** @deprecated need to add default indexes for checking it */
    public function checkIsTablesInCluster(): bool
    {
        return $this->searchdStatus['cluster_'.$this->clusterName.'_indexes'] === "pq,tests"
            || $this->searchdStatus['cluster_'.$this->clusterName.'_indexes'] === "tests,pq";
    }

    public function getViewNodes()
    {
        if ($this->searchdStatus === []) {
            $this->getStatus();
        }

        return $this->searchdStatus['cluster_'.$this->searchdStatus['cluster_name'].'_nodes_view'] ?? false;
    }

    public function isClusterPrimary()
    {
        if ($this->searchdStatus === []) {
            $this->getStatus();
        }

        return ($this->searchdStatus['cluster_'.$this->searchdStatus['cluster_name'].'_status'] === 'primary') ?? false;
    }

    public function createCluster($log = true): bool
    {
        $this->query('CREATE CLUSTER '.$this->clusterName, $log);

        if ($this->getConnectionError()) {
            return false;
        }

        $this->searchdStatus = [];
        $this->getStatus();

        return true;
    }

    public function addNotInClusterTablesIntoCluster()
    {
        $notInClusterTables = $this->getNotInClusterTables();
        if ($notInClusterTables !== []) {
            foreach ($notInClusterTables as $table) {
                $this->addTableToCluster($table);
                Logger::log("Table $table was added into cluster");
            }
        }
    }

    public function getNotInClusterTables()
    {
        $tables = $this->getTables();

        $clusterTables = $this->searchdStatus['cluster_'.$this->clusterName.'_indexes'];
        if ($clusterTables === '') {
            return $tables;
        }

        $clusterTables = explode(',', $clusterTables);

        $notInClusterTables = [];
        foreach ($clusterTables as $inClusterTable) {
            $inClusterTable = trim($inClusterTable);

            if ( ! in_array($inClusterTable, $tables)) {
                $notInClusterTables[] = $inClusterTable;
            }
        }

        return $notInClusterTables;
    }

    public function restoreCluster($log = true): bool
    {
        $this->query("SET CLUSTER GLOBAL 'pc.bootstrap' = 1", $log);

        if ($this->getConnectionError()) {
            return false;
        }

        $this->searchdStatus = [];
        $this->getStatus();

        return true;
    }


    public function joinCluster($hostname, $log = true): bool
    {
        if ($this->checkClusterName()) {
            return true;
        }
        $this->query('JOIN CLUSTER '.$this->clusterName.' at \''.$hostname.':9312\'', $log);

        if ($this->getConnectionError()) {
            return false;
        }

        $this->searchdStatus = [];
        $this->getStatus();

        return true;
    }

    public function createTable($tableName, $type): bool
    {
        if ( ! in_array($type, ['percolate', 'rt'])) {
            throw new \RuntimeException('Wrong table type '.$type);
        }

        if ( ! $this->fields) {
            throw new \RuntimeException('Fields was not initialized '.$tableName);
        }

        if ( ! $this->rtInclude) {
            throw new \RuntimeException('RT include was not initialized '.$tableName);
        }

        $this->query("CREATE TABLE IF NOT EXISTS $tableName (".implode(',',
                $this->fields).") type='$type' $this->rtInclude");

        if ($this->getConnectionError()) {
            return false;
        }

        return true;
    }

    public function addTableToCluster($tableName, $log = true): bool
    {
        $this->query("ALTER CLUSTER ".$this->clusterName." ADD ".$tableName, $log);

        if ($this->getConnectionError()) {
            return false;
        }

        $this->searchdStatus = [];
        $this->getStatus();

        return true;
    }

    public function connectAndCreate(): bool
    {
        if ($this->checkClusterName()) {
            if ( ! $this->checkIsTablesInCluster()) {
                if ($this->isTableExist('pq') && $this->isTableExist('tests')
                    && $this->addTableToCluster('pq')
                    && $this->addTableToCluster('tests')
                ) {
                    return true;
                }

                if ($this->createTable('pq', self::INDEX_TYPE_PERCOLATE)
                    && $this->addTableToCluster('pq')
                    && $this->createTable('tests', self::INDEX_TYPE_RT)
                    && $this->addTableToCluster('tests')
                ) {
                    return true;
                }

                return false;
            }

            return true;
        }

        if ($this->createCluster()
            && $this->createTable('pq', self::INDEX_TYPE_PERCOLATE)
            && $this->addTableToCluster('pq')
            && $this->createTable('tests', self::INDEX_TYPE_RT)
            && $this->addTableToCluster('tests')
        ) {
            return true;
        }

        return false;
    }

    public function setFields($rules)
    {
        $this->rtInclude = $this->getRtInclude();
        $fields          = ['`invalidjson` text indexed'];
        $envFields       = explode("|", $rules);
        foreach ($envFields as $field) {
            $field = explode("=", $field);
            if ( ! empty($field[0]) && ! empty($field[1])) {
                if ($field[0] === "text") {
                    $fields[] = "`".$field[1]."` ".$field[0]." indexed";
                } elseif ($field[0] === 'url') {
                    $fields[] = "`{$field[1]}_host_path` text indexed";
                    $fields[] = "`{$field[1]}_query` text indexed";
                    $fields[] = "`{$field[1]}_anchor` text indexed";
                } else {
                    $fields[] = "`".$field[1]."` ".$field[0];
                }
            }
        }

        $this->fields = $fields;
    }

    protected function getRtInclude()
    {
        $conf = '/etc/manticoresearch/conf_mount/rt_include.conf';
        if (file_exists($conf)) {
            return file_get_contents($conf);
        }

        return "charset_table = 'cjk, non_cjk'";
    }

    protected function query($sql, $logQuery = true, $attempts = 0)
    {
        $result = $this->connection->query($sql);

        if ($logQuery) {
            Logger::log('Query: '.$sql);
        }

        if ($this->getConnectionError()) {
            Logger::log("Error until query processing. Query: ".$sql."\n. Error: ".$this->getConnectionError());
            if ($attempts > $this->maxAttempts) {
                throw new \RuntimeException("Can't process query ".$sql);
            }

            sleep(1);
            $attempts++;

            return $this->query($sql, $logQuery, $attempts);
        }

        return $result;
    }

    public function reloadIndexes()
    {
        return $this->query('RELOAD INDEXES');
    }

    public function getChunksCount($index, $log = true): int
    {
        $indexStatus = $this->fetch('SHOW INDEX '.$index.' STATUS', $log);
        foreach ($indexStatus as $row) {
            if ($row["Variable_name"] === 'disk_chunks') {
                return (int) $row["Value"];
            }
        }
        throw new \RuntimeException("Can't get chunks count");
    }


    public function optimize($index, $cutoff)
    {
        return $this->query('OPTIMIZE INDEX '.$index.' OPTION cutoff='.$cutoff);
    }

    public function showThreads($log = true)
    {
        return $this->fetch('SHOW THREADS option format=all', $log);
    }

    private function fetch($query, $log = true)
    {
        $result = $this->query($query, $log);

        if ( ! empty($result)) {
            /** @var \mysqli_result $result */
            $result = $result->fetch_all(MYSQLI_ASSOC);
            if ($result !== null) {
                return $result;
            }
        }

        return false;
    }

    public function getConnectionError(): string
    {
        return $this->connection->error;
    }
}
