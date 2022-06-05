<?php

namespace Core\K8s;


use Core\Logger\Logger;
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
                Logger::log('K8s api don\'t respond');
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
                        Logger::log('Error pod phase '.json_encode($pod));
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

    public function getMinAvailableReplica()
    {
        $podsList = $this->getPodsHostnames();
        if ($podsList === []) {
            throw new \RuntimeException("Can't get available nodes list");
        }

        ksort($podsList);


        $min = array_shift($podsList);

        if ($min === gethostname()) {
            // skip itself
            $min = array_shift($podsList);
        }

        return $min;
    }

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
