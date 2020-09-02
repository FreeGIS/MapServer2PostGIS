const { Pool } = require('pg')
const getRandomUserAgent=require('./userAgent');
const superagent = require('superagent');

var pool,mapserver;
async function run(config){
    pool=new Pool(config.target);
    mapserver=config.source;
    const metedata= await getMapServerMetedata();

    for(let i=0;i<metedata.layers.length;i++){
        const layer=metedata.layers[i];
        if(layer.type!='Feature Layer')
            continue;
        const layerMetedata=await getLayerMetedata(layer);
        console.log(layerMetedata);
    }
}

//读取元数据
function getMapServerMetedata(){
    var p = new Promise(function(resolve, reject){
        const userAgent=getRandomUserAgent();
        const url=`${mapserver}/?f=pjson`;
        superagent.get(url)
            .set(userAgent).timeout({
                response: 30000,  
                deadline: 60000, 
            })      
            .retry(3)
            .end(function(err,res){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    if(res.status==200){
                        resolve(JSON.parse(res.text));
                    }
                    else{
                        reject('查询失败');
                    }
                }
            });
    }).then(undefined, (error) => {
        //错误不做额外处理
    });;
    return p;
}

//获取图层元数据
function getLayerMetedata(layer){
    var p = new Promise(function(resolve, reject){
        const layerserver=`${mapserver}/${layer.id}?f=pjson`;
        const userAgent=getRandomUserAgent();
        superagent.get(layerserver)
            .set(userAgent).timeout({
                response: 30000,  
                deadline: 60000, 
            })      
            .retry(3)
            .end(function(err,res){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    if(res.status==200){
                        const info=JSON.parse(res.text);
                        resolve({
                            id:info.id,
                            name:info.name,
                            srid:info.sourceSpatialReference.latestWkid,
                            geometryType:info.geometryType,
                            geometryField:info.geometryField.name,
                            fileds:info.fields
                        });
                    }
                    else{
                        reject('查询失败');
                    }
                }
            });
    }).then(undefined, (error) => {
        //错误不做额外处理
    });;
    return p;
    
}

//元数据layers建立对应的表
function layers2tables(layers){

}

function layertable(layer){

}

module.exports=run;