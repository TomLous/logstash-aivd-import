<?php
/**
 * @author      Tom Lous <tomlous@gmail.com>
 * @copyright   2016 Tom Lous
 * @package     package
 * Datetime:     06/02/16 13:45
 */

$filename = $argv[1];
$orgFilename = substr($filename,0,2).'/'.substr($filename,0,4).'/'.$filename;
$data = file_get_contents($filename);

$index = 'not-so-secret-agent-002';
$ip = '178.62.232.68';

$output = 'export/sentiment.csv';


//if(!file_exists($output)){
//    $df = fopen($output, 'w');
//    fprintf($df, chr(0xEF).chr(0xBB).chr(0xBF));
//}else{
//    $df = fopen($output, 'a');
//}
//fprintf($df, chr(0xEF).chr(0xBB).chr(0xBF));


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


$url = 'http://'.$ip.':9200/'.$index.'/'.$bucket;

//print $url.PHP_EOL;

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


$mongoClient = new \MongoClient('mongodb://'.$ip);
$mongoClient = new \MongoClient('mongodb://'.$ip);

$elasticCollection = $mongoClient->selectCollection('AIVD', 'elastic');
$elasticCollection->ensureIndex(array('elastic_id' => 1));
$elasticCollection->ensureIndex(array('filename' => 1));
$elasticCollection->ensureIndex(array('_type' => 1));

//fputcsv($df, array($resultDocument['elastic_id'], $filename, strip_tags($json['interaction']['content']), $json['language']['tag']), ';');

$elasticCollection->insert($resultDocument);

unlink($filename);




