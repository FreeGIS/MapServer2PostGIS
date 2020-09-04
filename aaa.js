const { Pool } = require('pg')
const superagent = require('superagent');

const mapserver='http://cumulus.tnc.org/arcgis/rest/services/CN_PAtool/PAtool/MapServer';
const pool=new Pool({
        "host":"106.54.197.46",
        "port":8433,
        "user":"postgres",
        "password":"yzg2020",
        "database":"nature"
    });


	let iii=[11,17];
for(let i=0;i<iii.length;i++){
	const url=`${mapserver}/12/query?where=OBJECTID=${iii[i]}&outFields=*&returnGeometry=true&f=geojson`;
	superagent.get(url).responseType('json')
        .timeout({
            response: 6000000,  
            deadline: 6000000, 
        }).retry(3)
	  .end(function(err,res){
            if(err){
                console.log('query',err);
                return;
            }
            const datas=res.body.toString();
            let dataSet;
            try{
                dataSet=JSON.parse(datas);
            }catch(err1){
                console.log('parse',err1);
               return;
            } 
			
			const feature=dataSet.features[0];
			let sql;
			if(feature.geometry.type==='MultiPolygon')
				sql=`with polygon_geom as (
			select ST_GeomFromGeoJSON('${JSON.stringify(feature.geometry)}') as geom
		) INSERT INTO test_12(
			name,geom) select '${feature.properties.name}',st_geometryN(a.geom,n) from polygon_geom a CROSS JOIN generate_series (1, 5000) n 
			where  n <= ST_NumGeometries(a.geom)`;
			else
				sql=`insert into test_12(name,geom) values ('${feature.properties.name}',ST_GeomFromGeoJson('${JSON.stringify(feature.geometry)}'))`;
			pool.query(sql,function(err1,res1){
				 if(err1){
                console.log('insert',err1);
               
            }
			else{
				console.log(iii[i]);
			}
			});
	  });
}




//1 3 6 9 16 20 21 22 23 25 26 27 28 29 31 34 35 36
 


