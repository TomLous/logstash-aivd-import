<?php
/**
 * @author      Tom Lous <tomlous@gmail.com>
 * @copyright   2016 Tom Lous
 * @package     package
 * Datetime:     06/02/16 13:45
 */
date_default_timezone_set('Europe/Amsterdam');
$filename = $argv[1];
$orgFilename = substr($filename,0,2).'/'.substr($filename,0,4).'/'.$filename;
$data = file_get_contents($filename);

$index = 'not-so-secret-agent-013';
$ip = '178.62.232.68';


$json = json_decode($data, true);

$keys = array_keys($json);

$bucket = 'unknown';

foreach($keys as $key){
    if(in_array($key, array('blogs', 'blog', 'facebook_page', 'board', 'facebook_user', 'geotag', 'post', 'reddit', 'facebook', 'sinaweibo', 'tencentweibo', 'thread', 'tumblr', 'twitter',
        'video', 'wordpress', 'youtube'))){
        $bucket = $key;
    }
}

if($bucket == 'unknown'){
    print_r($json);
}

preg_match_all('/latitude\":\s*([0-9\.]+)/', $data, $matches);
$newData = array();
if(isset($matches[1]) && isset($matches[1][0])){
    $lat = $matches[1][0];


    preg_match_all('/longitude\":\s*([0-9\.]+)/', $data, $matches);
    $long = $matches[1][0];

    $newData['pin'] = array("location" => array("lat"=> $lat, "lon"=>$long));

}

//print_r($json);

$time = strtotime($json['interaction']['created_at']);

$newData['date'] =  date('c',$time);
$newData['date2'] =  date('Y-m-d H:i:s',$time);
$newData['timestamp'] =  $time;




$d2 = substr(json_encode($newData),1,-1);
$data = substr($data, 0, -1).','.$d2.'}';

//$data = json_encode($json);

//print_r($data);

$url = 'http://'.$ip.':9200/'.$index.'/'.$bucket;

print $url.PHP_EOL;

$curl_handle=curl_init();
curl_setopt($curl_handle,CURLOPT_URL,$url);
curl_setopt($curl_handle,CURLOPT_CONNECTTIMEOUT,2);
curl_setopt($curl_handle, CURLOPT_CUSTOMREQUEST, "POST");
curl_setopt($curl_handle, CURLOPT_POSTFIELDS, $data);
curl_setopt($curl_handle, CURLOPT_RETURNTRANSFER,true);
curl_setopt($curl_handle, CURLOPT_SSL_VERIFYPEER, false);
curl_setopt($curl_handle, CURLOPT_HTTPHEADER, array(
    'Content-Type: application/json',
    'Content-Length: ' . strlen($data)
));
$result = curl_exec($curl_handle);


//print_r($result);



$resultDocument = json_decode($result, true);


$resultDocument['elastic_id'] = $resultDocument['_id'];
unset($resultDocument['_id']);

$resultDocument['filename'] = $orgFilename;
$resultDocument['interaction'] =  $json['interaction'];
if(isset($json['language'])) {
    $resultDocument['language'] = $json['language'];
}
if(isset($json['demographics'])) {
    $resultDocument['demographics'] = $json['demographics'];
}

$resultDocument['date'] =  date('c',$time);
$resultDocument['date2'] =  date('Y-m-d H:i:s',$time);
$resultDocument['_timestamp'] =  $time;

$mongoClient = new \MongoClient('mongodb://'.$ip);
//$mongoClient = new \MongoClient('mongodb://'.$ip);




$elasticCollection = $mongoClient->selectCollection('AIVD', 'elastic');
$elasticCollection->ensureIndex(array('elastic_id' => 1));
$elasticCollection->ensureIndex(array('filename' => 1));
$elasticCollection->ensureIndex(array('_type' => 1));

//fputcsv($df, array($resultDocument['elastic_id'], $filename, strip_tags($json['interaction']['content']), $json['language']['tag']), ';');

$elasticCollection->insert($resultDocument);

unlink($filename);




