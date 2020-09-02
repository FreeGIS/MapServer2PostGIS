const { Pool } = require('pg')
const getRandomUserAgent=require('./userAgent');
const superagent = require('superagent');

var pool,mapserver;
async function run(config){
    pool=new Pool(config.target);
    mapserver=config.source;
    //获取元数据
    const metedata= await getMapServerMetedata();
    //基于元数据建表
    await creat_spatial_tables(metedata.layers);
    //数据同步

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
async function creat_spatial_tables(layers){
    let promises=[]
    for(let i=0;i<layers.length;i++){
        const layer=layers[i];
        if(layer.type!='Feature Layer')
            continue;
        const layerMetedata=await getLayerMetedata(layer);
        promises.push(creat_spatial_table(layerMetedata));
    }
    await Promise.all(promises);
}

function creat_spatial_table(layerMetedata){
    var p = new Promise(function(resolve, reject){
        const tablename='test_'+layerMetedata.id;
        //拼接字段,不要objectid shape length area
        let field_sql='';
        for(let i=0;i<layerMetedata.fileds.length;i++){
            const field=layerMetedata.fileds[i];
            const field_name = field.name.toLowerCase();
            //在循环的时候，获取主键字段，并绑定给元数据引用对象。
            if(field.type==='esriFieldTypeOID')
            {
                layerMetedata.oid_key=field.name;
                continue;
            }
            if(field_name.startsWith('shape')||field_name.startsWith('objectid')||field_name.startsWith('length')||field_name.startsWith('area'))
                continue;
            //类型转换
            const field_type = type_Convert(field.type);
            if(CheckChinese(field_name))
                field_sql+=`"${field_name}" ${field_type},\n`;
            else
                field_sql+=`${field_name} ${field_type},\n`;
        }
        //图形转换
        if(layerMetedata.geometryType=='esriGeometryPoint')
            field_sql+=`geom geometry(Point,${layerMetedata.srid})`;
        else if(layerMetedata.geometryType=='esriGeometryPolyline')
            field_sql+=`geom geometry(LineString,${layerMetedata.srid})`;
        if(layerMetedata.geometryType=='esriGeometryPolygon')
            field_sql+=`geom geometry(Polygon,${layerMetedata.srid})`;

        let sql=`create table ${tablename}(
            gid serial primary key,
            ${field_sql}
        );
        create index ${tablename}_geom_idx on ${tablename} using gist(geom);
        `;
        pool.query(sql,function(err,res){
            if(err){
                console.log(err);
                resolve(err);
            }
            else
                reject('SUCCESS');
            
        });
    }).then(undefined, (error) => {
        //错误不做额外处理
    });
    return p;
}



async function getLayerFeatureCount(id){
    try{
        const url= `mapserver/${id}/query?where=1=1&returnCountOnly=true&f=pjson`;
        const userAgent=getRandomUserAgent();
        const res = await superagent.get(url)
                .set(userAgent).timeout({
                    response: 30000,  
                    deadline: 60000, 
                }); 
        const result =JSON.parse(res); 
        return result.count;   
    }
    catch(err){
        console.log(err);
        return null;
    }
    
}
async function features2Postgis(layerid){
    //获取服务的要素总数
    const feature_count=getLayerFeatureCount(layerid);
    //根据总数，每1000行数据为分页查询
    for(let i=0;i<feature_count;i=i+1000){

    }
    //查询数据入库
}





//http://cumulus.tnc.org/arcgis/rest/services/CN_PAtool/PAtool/MapServer/0/query?where=1=1&returnCountOnly=true&f=pjson
//esri结构转pg结构
function type_Convert(type){
	let pg_type;
	switch(type){
		case 'esriFieldTypeDouble':
			pg_type='numeric';
			break;
		case 'esriFieldTypeString':
			pg_type='text';
			break;
		case 'esriFieldTypeSmallInteger':
			pg_type='integer';
			break;
		case 'esriFieldTypeInteger':
			pg_type='integer';
			break;
		case 'esriFieldTypeSingle':
			pg_type='numeric';
			break;
		case 'esriFieldTypeDate':
			pg_type='date';
			break;
		case 'esriFieldTypeOID':
			pg_type='int';
			break;
		case 'esriFieldTypeBlob':
			pg_type='blob';
			break;	
		default:
			pg_type=type;
			break;	
									
	}
	return pg_type;
}

function CheckChinese(val){
    var reg = new RegExp("[\\u4E00-\\u9FFF]+","g");
    if(reg.test(val)){
        return true;
    }
    return false;
}
module.exports=run;