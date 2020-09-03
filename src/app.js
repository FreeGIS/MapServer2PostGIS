const { Pool } = require('pg')
const getRandomUserAgent=require('./userAgent');
const superagent = require('superagent');

var pool,mapserver;
async function run(config){
    pool=new Pool(config.target);
    mapserver=config.source;
    //获取服务元数据
    const metedata= await getMapServerMetedata();
    //获取图层元数据
    const layerMetedatas=await getLayerMetedatas(metedata.layers);
    //基于元数据建表
    await creat_spatial_tables(layerMetedatas);
    //layerMetedatas.length
    //12 15 
    //for(let i=12;i<16;i++){
        //数据同步
        await features2Postgis(layerMetedatas[15]);
       // break;
    //}
    console.log('finish');
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
                            tablename:'test_'+info.id,
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
//获取图层元数据
async function getLayerMetedatas(layers){
    let promises=[]
    for(let i=0;i<layers.length;i++){
        const layer=layers[i];
        if(layer.type!='Feature Layer')
            continue;
        promises.push(getLayerMetedata(layer));
    }
    const layerMetedatas=await Promise.all(promises);
    return layerMetedatas;
}

//元数据layers建立对应的表
async function creat_spatial_tables(layerMetedatas){
    let promises=[]
    for(let i=0;i<layerMetedatas.length;i++){
        const layerMetedata=layerMetedatas[i];
        promises.push(creat_spatial_table(layerMetedata));
    }
    await Promise.all(promises);
}

function creat_spatial_table(layerMetedata){
    var p = new Promise(function(resolve, reject){
        const tablename=layerMetedata.tablename;
        layerMetedata.columns={};
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
            layerMetedata.columns[field_name]=field_type;
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

        let sql=`
        drop table if exists ${tablename};
        create table ${tablename}(
            gid serial primary key,
            ${field_sql}
        );
        create index ${tablename}_geom_idx on ${tablename} using gist(geom);
        COMMENT ON TABLE ${tablename} IS '${layerMetedata.name}';
        `;
        //pool.query(sql,function(err,res){
            if(!sql){
                console.log(err);
                resolve(err);
            }
            else{
                console.log(`服务 ${layerMetedata.name} ->表 ${tablename} 创建完毕！`)
                reject('SUCCESS');
            }
        //});
    }).then(undefined, (error) => {
        //错误不做额外处理
    });
    return p;
}



async function getLayerFeatureCount(id){
    try{
        const url= `${mapserver}/${id}/query?where=1=1&returnCountOnly=true&f=pjson`;
        const userAgent=getRandomUserAgent();
        const res = await superagent.get(url)
                .set(userAgent).timeout({
                    response: 30000,  
                    deadline: 60000, 
                }).retry(3); 
        const result =JSON.parse(res.text); 
        return result.count;   
    }
    catch(err){
        console.log(err);
        return null;
    }
    
}

function getDate2pg(url,sql_header,layer){
    var p = new Promise(function(resolve, reject){
        const userAgent=getRandomUserAgent();
        superagent.get(url).responseType('json')
        .set(userAgent).timeout({
            response: 30000,  
            deadline: 60000, 
        }).retry(3)
        .end(function(err,res){
            if(err){
                console.log(url);
                resolve(err);
            }
            const datas=res.body.toString();
            let dataSet;
            try{
                dataSet=JSON.parse(datas);
            }catch(err1){
                console.log(url);
                resolve(err1);
                return;
            } 
            let sql=sql_header;
            if(!dataSet.features){
               // console.log('features null',dataSet);
                console.log('features null',url);
                resolve('null');
                return;
            }
            //数据转换
            for(let j=0;j<dataSet.features.length;j++){
                let values='(';
                const feature=dataSet.features[j];
                const attributes=feature.attributes;
                const esri_geom=feature.geometry;
                for(let key in attributes){
                    const column_name=key.toLowerCase();
                    const column_type=layer.columns[column_name];
                    if(!column_type)
                        continue;
                    else{
                        if(column_type==='text')
                            values+=`'${value_format(attributes[key])}',`;
                        else 
                            values+=`${attributes[key]},`;
    
                        /*
                        else if(column_type==='date'){
    
                        }*/
                    }
                }
                //图形转换
                let geom;
                switch (layer.geometryType) {
                    case 'esriGeometryPoint':
                        geom = createPT(esri_geom);
                        break;
                    case 'esriGeometryPolyline':
                        geom = createLineString(esri_geom);
                        break;
                    case 'esriGeometryPolygon':
                        geom = createPolygon(esri_geom);
                        break;
                }
                if(j!=dataSet.features.length-1)
                    values+=`ST_GeomFromText('${geom}', ${layer.srid})),`;
                else
                    values+=`ST_GeomFromText('${geom}', ${layer.srid}));`;
                sql+=values;
            }
            pool.query(sql,function(db_err,db_res){
                if(db_err){
                    console.log(db_err);
                    resolve(db_err);
                }
                //console.log(`insert into ${layer.tablename} counts:${(i+1)*500}/${feature_count}`);
                reject('SUCCESS');
            });
        }); 

    }).then(undefined, (error) => {
        //错误不做额外处理
    });
    return p;
}

async function features2Postgis(layer){
    //获取服务的要素总数
    const feature_count=await getLayerFeatureCount(layer.id);
    const pages=Math.ceil(feature_count/100.0);//总页数
    const tablename=layer.tablename;
    let sql_header=`insert into ${tablename}(`;
    for(let key in layer.columns){
        if(CheckChinese(key))
            sql_header+=`"${key}",`;
        else
            sql_header+=`${key},`;
    }
    sql_header+='geom) values ';
    //根据总数，每1000行数据为分页查询
    let promises=[];
   //const urls=["http://cumulus.tnc.org/arcgis/rest/services/CN_PAtool/PAtool/MapServer/19/query?where=OBJECTID_1>2200 and OBJECTID_1<=2300&outFields=*&returnGeometry=true&f=pjson"];    
   for(let i=0;i<pages;i++){
   // for(let i=0;i<1;i++){
        const where=`${layer.oid_key}>${i*100} and ${layer.oid_key}<=${(i+1)*100}`;
        const url=`${mapserver}/${layer.id}/query?where=${where}&outFields=*&returnGeometry=true&f=pjson`;
        //promises.push(getDate2pg(urls[i],sql_header,layer));
        promises.push(getDate2pg(url,sql_header,layer));
        if(promises.length==40){
            await Promise.all(promises);
            console.log(`insert into ${layer.tablename} counts:${(i+1)*100}/${feature_count}`);
            promises=[];
        }
    }
    if(promises.length>0){
        await Promise.all(promises);
        promises=[];
    }
    console.log(tablename+'入库完毕！');
    
}

function value_format(_value){
	_value=_value.replace(/'/g,"''");
	return _value;
}



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
//构造点
function createPT(geom){
	return `Point(${geom.x} ${geom.y})`;
}
//构造线
function createLineString(geom){
	let paths=geom.paths.map(function(path){
		path=path.map(function(coor){
			return coor.join(' ');
		});
		return '('+path.join(',')+')';
	});
	return `LineString${paths}`;
}
//构造面
function createPolygon(geom){
	let rings=geom.rings.map(function(ring){
		ring=ring.map(function(coor){
			return coor.join(' ');
		});
		return '('+ring.join(',')+')';
	});
	return `Polygon(${rings})`;
}

function CheckChinese(val){
    var reg = new RegExp("[\\u4E00-\\u9FFF]+","g");
    if(reg.test(val)){
        return true;
    }
    return false;
}
module.exports=run;