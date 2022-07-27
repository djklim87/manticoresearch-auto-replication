<?php

namespace Core\Manticore;

use Core\K8s\Resources;
use Core\Logger\Logger;

class ManticoreJson
{
    private $conf = [];
    private string $path;
    private string $clusterName;

    public function __construct($clusterName)
    {
        $this->clusterName = $clusterName;
        if (defined('DEV')) {
            $this->conf = [

                "clusters" => [
                    "m_cluster" => [
                        "nodes" => "192.168.0.1:9312,92.168.0.1:9312",
                        "options" => "",
                        "indexes" => ["pq", "tests"],
                    ],
                ],

                "indexes" => [
                    "pq" => [
                        "type" => "percolate",
                        "path" => "pq",
                    ],
                    "tests" => [
                        "type" => "rt",
                        "path" => "tests",
                    ],

                ],
            ];
            $this->path = '/tmp/manticore.json';
        } else {
            $this->path = '/var/lib/manticore/manticore.json';

            if (file_exists($this->path)) {
                try {
                    $manticoreJson = file_get_contents($this->path);
                    Logger::log("Manticore json content: ".$manticoreJson);
                    $this->conf = json_decode($manticoreJson, true);
                } catch (\Exception $exception) {
                    $this->conf = [];
                }
            } else {
                $this->conf = [];
            }
        }
    }


    public function hasCluster(): bool
    {
        return isset($this->conf['clusters'][$this->clusterName]);
    }

    public function getClusterNodes()
    {
        if (!isset($this->conf['clusters'][$this->clusterName]['nodes'])) {
            return [];
        }
        $nodes = $this->conf['clusters'][$this->clusterName]['nodes'];

        return explode(',', $nodes);
    }

    public function updateNodesList(array $nodesList): void
    {
        echo "=> Update nodes list ".json_encode($nodesList)."\n";
        if ($nodesList !== []){
            $newNodes = implode(',', $nodesList);

            if (!isset($this->conf['clusters'][$this->clusterName]['nodes']) ||
                $newNodes !== $this->conf['clusters'][$this->clusterName]['nodes']) {

                $this->conf['clusters'][$this->clusterName]['nodes'] = $newNodes;
                $this->save();
            }
        }
    }

    public function getConf(){
        return $this->conf;
    }

    public function startManticore()
    {
        exec('supervisorctl start searchd');
    }

    public function checkNodesAvailability(Resources $resources, $port, $label, $attempts): void
    {
        $nodes = $resources->getPodsHostnames();

        $availableNodes = [];
        foreach ($nodes as $node) {
            // Skip current node
            if ($node === gethostname()) {
                continue;
            }

            try {
                $connection = new ManticoreConnector($node, $port, $label, $attempts);
                if (!$connection->checkClusterName()) {
                    Logger::log("Cluster name mismatch at $node");
                    continue;
                }
                $availableNodes[] = $node.':'.$port;
            } catch (\RuntimeException $exception) {
                echo "=> Node at $node no more available\n".$exception->getMessage();
            }
        }

        $this->updateNodesList($availableNodes);
    }

    /**
     * @throws \JsonException
     */
    private function save(): void
    {
        echo "=> Save manticore.json ".json_encode($this->conf)."\n";
        file_put_contents($this->path, json_encode($this->conf, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }
}
