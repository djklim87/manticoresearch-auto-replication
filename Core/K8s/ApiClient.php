<?php

namespace Core\K8s;

use GuzzleHttp\Client;
use GuzzleHttp\Psr7;
use GuzzleHttp\Exception\RequestException;
use JsonException;

class ApiClient
{

    public const API_URL_SCHEME_NO_NS = '{{API-URL}}/{{API-VERSION}}/{{API-SECTION}}';
    public const API_URL_SCHEME = '{{API-URL}}/{{API-VERSION}}/namespaces/{{NAMESPACE}}/{{API-SECTION}}';
    public const TYPE_STATEFULSET = 'statefulsets';
    public const TYPE_SERVICE = 'services';
    public const TYPE_PODS = 'pods';
    public const TYPE_NODES = 'nodes';


    private string $apiUrl = 'https://kubernetes.default.svc';
    private string $cert = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt';
    private array $apiSections
        = [
            self::TYPE_SERVICE       => 'api/v1',
            self::TYPE_STATEFULSET   => 'apis/apps/v1',
            'configmaps'             => 'api/v1',
            'persistentvolumeclaims' => 'api/v1',
            'secrets'                => 'api/v1',
            self::TYPE_PODS          => 'api/v1',
            self::TYPE_NODES         => 'api/v1',
        ];

    private string $bearer;
    private Client $httpClient;
    private string $userAgent;
    private $namespace;

    public function __construct()
    {
        $this->bearer    = $this->getBearer();
        $this->userAgent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '.
            '(KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36';

        $this->namespace  = $this->getNamespace();
        $this->httpClient = new Client();
    }

    /**
     * @throws JsonException
     */
    public function getManticorePods()
    {
        return json_decode($this->request(self::TYPE_PODS)->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * @throws JsonException
     */
    public function getNodes()
    {
        return json_decode($this->request(self::TYPE_NODES, 'GET', true)->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
    }

    private function request($section, $type = "GET", $noNamespace = false)
    {
        $params = [
            'verify'  => $this->cert,
            'version' => 2.0,
            'headers' => [
                'Authorization' => 'Bearer '.$this->bearer,
                'Accept'        => 'application/json',
                'User-Agent'    => $this->userAgent,
            ],
        ];

        return $this->call($type, $this->getUrl($section, $noNamespace), $params);
    }


    private function getUrl($section, $noNamespace = false)
    {
        if ($noNamespace) {
            return str_replace(['{{API-URL}}', '{{API-VERSION}}', '{{API-SECTION}}'],
                [$this->apiUrl, $this->apiSections[$section], $section], self::API_URL_SCHEME_NO_NS);
        }

        return str_replace(['{{API-URL}}', '{{API-VERSION}}', '{{NAMESPACE}}', '{{API-SECTION}}'],
            [$this->apiUrl, $this->apiSections[$section], $this->namespace, $section], self::API_URL_SCHEME);
    }


    private function getBearer(): string
    {
        $bearerFile = '/var/run/secrets/kubernetes.io/serviceaccount/token';
        if (file_exists($bearerFile)) {
            return file_get_contents($bearerFile);
        }

        return false;
    }


    private function getNamespace()
    {
        $bearerFile = '/var/run/secrets/kubernetes.io/serviceaccount/namespace';
        if (file_exists($bearerFile)) {
            return file_get_contents($bearerFile);
        }

        return false;
    }

    public function get($url)
    {
        return $this->call('GET', $url);
    }

    private function call($method, $url, $params = [])
    {
        try {
            return $this->httpClient->request($method, $url, $params);
        } catch (RequestException $e) {
            echo Psr7\Message::toString($e->getRequest())."\n";
            if ($e->hasResponse()) {
                echo Psr7\Message::toString($e->getResponse());
            }

            exit(1);
        }
    }
}
