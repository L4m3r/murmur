import {primaryColumns,keyOfRow} from './keys.js';
// A global secondary index is an ordinary ordered table: indexed columns first,
// followed by primary-key columns not already present (YDB's dedup rule).
export function indexKeyColumns(schema,index){
 const columns=primaryColumns(schema);
 if(columns.length===1)return index.column===columns[0]?['value']:['value','pk'];
 return ['value',...columns.flatMap((column,i)=>column===index.column?[]:[`pk${i}`])];
}
export function indexEntry(schema,index,row){
 if(!row)return null;
 const columns=primaryColumns(schema),entry={value:row[index.column],pk:keyOfRow(schema,row)};
 if(columns.length>1)columns.forEach((column,i)=>{entry[`pk${i}`]=row[column];});
 return entry;
}
export function insertKeys(schema,statement){
 const columns=primaryColumns(schema),positions=columns.map(column=>statement.columns.indexOf(column));
 if(positions.some(i=>i<0))throw new Error('INSERT requires every primary-key column');
 return statement.values.map(values=>columns.length===1?values[positions[0]]:positions.map(i=>values[i]));
}

/** Logical catalog actor names stay stable; row/index actors never reuse a dropped generation. */
export function physicalTableName(table,schema){return schema?.storageName||table;}
