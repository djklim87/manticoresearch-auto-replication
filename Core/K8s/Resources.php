<?php

namespace Core\K8s;


use Core\Notifications\NotificationInterface;

class Resources
{
    private $label;
    private $api;
    private $notification;
    private $pods;

    public function __construct(ApiClient $api, $label, NotificationInterface $notification)
    {
        $this->label        = $label;
        $this->api          = $api;
        $this->notification = $notification;
    }

    public function getPods()
    {
        if ( ! $this->pods) {
            $pods = $this->api->getManticorePods();
            if ( ! isset($pods['items'])) {
                echo "\n\n=> K8s api don't respond\n";
                exit(1);
            }

            foreach ($pods['items'] as $pod) {
                if (isset($pod['metadata']['labels']['label'])
                    && $pod['metadata']['labels']['label'] === $this->label
                ) {
                    if ($pod['status']['phase'] === 'Running' || $pod['status']['phase'] === 'Pending') {
                        $this->pods[] = $pod;
                    } else {
                        $this->notification->sendMessage("Bad pod phase for ".$pod['metadata']['name'].' phase '.$pod['status']['phase']);
                        echo "=> Error pod phase ".json_encode($pod)."\n\n";
                    }
                }
            }
        }

        return $this->pods;
    }

    public function getPodsHostnames(): array
    {
        if (defined('DEV') && DEV === true) {
            return [];
        }
        $hostnames = [];
        $this->getPods();

        foreach ($this->pods as $pod) {
            if ($pod['status']['phase'] === 'Running' || $pod['status']['phase'] === 'Pending') {
                $hostnames[] = $pod['metadata']['name'];
            }
        }

        return $hostnames;
    }

//    public function getMinReplica()
//    {
//        $this->getPods();
//
//
//        $min   = [];
//        $count = 0;
//
//        foreach ($this->pods['items'] as $pod) {
//            if (isset($pod['metadata']['labels']['label'])
//                && $pod['metadata']['labels']['label'] === $this->label
//            ) {
//                if ($pod['status']['phase'] === 'Running' || $pod['status']['phase'] === 'Pending') {
//                    if ( ! isset($pod['status']["podIP"])) {
//                        sleep(2);
//                        echo "One pod in pending state. Wait until it came alive...\n";
//
//                        return $this->getMinReplica();
//                    }
//                    $key       = strtotime($pod['status']['startTime']);
//                    $min[$key] = $pod['status']["podIP"];
//
//                    $count++;
//                } else {
//                    echo json_encode($pod)."\n\n";
//                }
//            }
//        }
//        echo "Replica hook: Pods count:".$count."\n";
//
//        return [$min, $count];
//    }

    public function getMinReplicaName(): string
    {
        $hostname = gethostname();
        $parts    = explode("-", $hostname);
        array_pop($parts);
        $parts[] = 0;

        return implode('-', $parts);
    }

    public function getCurrentReplica(): int
    {
        if (defined('DEV') && DEV === true) {
            return 0;
        }
        $hostname = gethostname();
        $parts    = explode("-", $hostname);

        return (int) array_pop($parts);
    }
}
