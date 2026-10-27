import { compareKeys } from './keys.js';
/** Deliberately bounded SQL subset. No eval, implicit casts, joins or subqueries. */
export function parse(sql) {
  if (typeof sql !== 'string' || sql.length > 65536) throw new Error('SQL must be a string of at most 65536 characters');
  const tokens = []; let i = 0;
  while (i < sql.length) {
    if (/\s/.test(sql[i])) { i++; continue; }
    const rest = sql.slice(i);
    if (rest.startsWith('--')) { const end = sql.indexOf('\n', i); i = end < 0 ? sql.length : end + 1; continue; }
    let m;
    if (sql[i] === "'") {
      let value = ''; i++; let closed = false;
      while (i < sql.length) { if (sql[i] === "'") { if (sql[i+1] === "'") { value += "'"; i += 2; } else { i++; closed = true; break; } } else value += sql[i++]; }
      if (!closed) throw new Error('Unterminated string'); tokens.push({kind:'literal',value});
    } else if ((m = rest.match(/^\d+(?:\.\d+)?/))) { tokens.push({kind:'literal',value:Number(m[0])}); i += m[0].length; }
    else if ((m = rest.match(/^[a-zA-Z_][a-zA-Z_0-9]*/))) { tokens.push({kind:'word',value:m[0].toLowerCase()}); i += m[0].length; }
    else if ((m = rest.match(/^(?:<=|>=|!=|<>|[(),;*=<>+\-])/))) { tokens.push({kind:'symbol',value:m[0]}); i += m[0].length; }
    else throw new Error(`Unsupported SQL character at ${i}`);
  }
  let pos = 0;
  const peek = () => tokens[pos]?.value;
  const take = v => { if (peek() === v) { pos++; return true; } return false; };
  const need = v => { if (!take(v)) throw new Error(`Expected ${v}, got ${peek() ?? 'end'}`); };
  const name = () => { const t = tokens[pos++]; if (t?.kind !== 'word' || ['__proto__','constructor','prototype'].includes(t.value)) throw new Error('Expected safe identifier'); return t.value; };
  const literal = () => { const neg = take('-'); const t = tokens[pos++]; let v;
    if (t?.kind === 'literal') v = t.value;
    else if (t?.value === 'null') v = null;
    else if (t?.value === 'true' || t?.value === 'false') v = t.value === 'true';
    else throw new Error('Expected literal');
    if (neg) { if (typeof v !== 'number') throw new Error('Unary minus requires number'); v = -v; } return v;
  };
  const where = () => { const out = []; if (take('where')) do { const column = name(); const op = peek(); if (!['=','!=','<>','<','>','<=','>='].includes(op)) throw new Error('Expected comparison'); pos++; out.push({column,op,value:literal()}); } while (take('and')); return out; };
  const list = fn => { const out = [fn()]; while (take(',')) out.push(fn()); return out; };
  const statements = []; let wrapper = false; let committed = false;
  while (pos < tokens.length) {
    if (take(';')) continue;
    if (take('begin')) { if (wrapper || statements.length || committed) throw new Error('BEGIN must precede statements'); take('transaction'); wrapper = true; need(';'); continue; }
    if (take('commit')) { if (!wrapper || committed) throw new Error('Unexpected COMMIT'); committed = true; take(';'); if (pos !== tokens.length) throw new Error('COMMIT must be last'); break; }
    let s;
    if (take('create')) {
      const unique = take('unique');
      if (unique || peek() === 'index') {
        need('index'); const index = name(); need('on'); const table = name(); need('('); const column = name(); need(')');
        s = {kind:'createIndex',table,index,column,unique};
      } else {
      need('table'); const table = name(); need('(');
      const columns = list(() => { const column = {name:name(), type:name().toUpperCase(),primaryKey:false,notNull:false};
        if (!['INTEGER','REAL','TEXT','BOOLEAN'].includes(column.type)) throw new Error('Unsupported column type');
        while (['primary','not'].includes(peek())) { if (take('primary')) { need('key'); column.primaryKey = true; column.notNull = true; } else { need('not'); need('null'); column.notNull = true; } } return column;
      }); need(')');
      if (new Set(columns.map(c=>c.name)).size !== columns.length || columns.filter(c=>c.primaryKey).length !== 1) throw new Error('Require unique column names and exactly one inline PRIMARY KEY');
      s = {kind:'create',table,columns};
      }
    } else if (['insert','upsert'].includes(peek())) {
      const kind = tokens[pos++].value; need('into'); const table = name(); need('('); const columns = list(name); need(')'); need('values');
      const values = list(() => { need('('); const row = list(literal); need(')'); return row; });
      s = {kind,table,columns,values};
    } else if (take('select')) {
      const columns = take('*') ? '*' : list(name); need('from'); const table = name(); const filter = where(); let order = null, limit = null;
      if (take('order')) { need('by'); order = {column:name(),desc:take('desc')}; if (!order.desc) take('asc'); }
      if (take('limit')) { limit = literal(); if (!Number.isSafeInteger(limit) || limit < 0 || limit > 10000) throw new Error('LIMIT must be integer 0..10000'); }
      s = {kind:'select',table,columns,filter,order,limit};
    } else if (take('update')) {
      const table = name(); need('set'); const assignments = list(() => { const column = name(); need('='); let expression;
        if (tokens[pos]?.kind === 'word' && !['null','true','false'].includes(peek())) { expression = {column:name()}; if (['+','-'].includes(peek())) { expression.op = tokens[pos++].value; expression.value = literal(); } }
        else expression = {value:literal()}; return {column,expression}; }); s = {kind:'update',table,assignments,filter:where()};
    } else if (take('delete')) { need('from'); const table = name(); s = {kind:'delete',table,filter:where()}; }
    else throw new Error(`Unsupported statement: ${peek()}`);
    statements.push(s); if (statements.length > 32) throw new Error('At most 32 statements per transaction');
    if (pos < tokens.length) need(';');
  }
  if (wrapper && !committed) throw new Error('BEGIN requires COMMIT');
  if (!statements.length) throw new Error('No statements');
  return statements;
}

export function execute(statements, catalog, tables) {
  const results = [];
  const known = (schema, name) => { const c = schema.columns.find(c=>c.name === name); if (!c) throw new Error(`Unknown column ${name}`); return c; };
  const check = (c,v) => {
    if (v === null) { if (c.notNull || c.primaryKey) throw new Error(`${c.name} cannot be NULL`); return; }
    const valid = c.type === 'INTEGER' ? Number.isSafeInteger(v) : c.type === 'REAL' ? typeof v === 'number' && Number.isFinite(v) : c.type === 'BOOLEAN' ? typeof v === 'boolean' : typeof v === 'string';
    if (!valid) throw new Error(`Invalid ${c.type} value for ${c.name}`);
    if (typeof v === 'string') {
      if (v.length > 8192) throw new Error('Text exceeds 8192 characters');
      // UTF-8 replacement of lone UTF-16 surrogates would alias distinct keys.
      for (let i = 0; i < v.length; i++) {
        const code = v.charCodeAt(i);
        if (code >= 0xd800 && code <= 0xdbff) {
          const low = v.charCodeAt(++i);
          if (!(low >= 0xdc00 && low <= 0xdfff)) throw new Error('TEXT requires well-formed UTF-16');
        } else if (code >= 0xdc00 && code <= 0xdfff) throw new Error('TEXT requires well-formed UTF-16');
      }
    }
  };
  const validateIndexes = (schema, rows) => {
    for (const index of schema.indexes || []) {
      if (!index.unique) continue;
      const seen = new Set();
      for (const row of rows) {
        const value = row[index.column];
        if (value === null) continue;
        if (seen.has(value)) throw new Error(`Unique index ${index.name} violation`);
        seen.add(value);
      }
    }
  };
  for (const s of statements) {
    if (s.kind === 'create') {
      if (Object.hasOwn(catalog,s.table)) throw new Error(`Table ${s.table} exists`);
      if (Object.keys(catalog).length >= 32) throw new Error('At most 32 tables');
      catalog[s.table] = {columns:s.columns,primaryKey:s.columns.find(c=>c.primaryKey).name,indexes:[]}; tables[s.table] = []; results.push({rowsAffected:0}); continue;
    }
    const schema = Object.hasOwn(catalog,s.table) && catalog[s.table]; if (!schema) throw new Error(`Unknown table ${s.table}`);
    const rows = tables[s.table]; const pk = schema.primaryKey;
    if (s.kind === 'createIndex') {
      known(schema,s.column);
      const indexes = schema.indexes || [];
      if (indexes.some(index => index.name === s.index)) throw new Error(`Index ${s.index} exists`);
      if (indexes.length >= 16) throw new Error('At most 16 indexes per table');
      schema.indexes = [...indexes,{name:s.index,column:s.column,unique:s.unique}];
      validateIndexes(schema,rows);
      results.push({rowsAffected:0}); continue;
    }
    for (const f of s.filter || []) { const c = known(schema,f.column); check({...c,notNull:false,primaryKey:false},f.value); }
    const matches = row => (s.filter || []).every(f => { const a = row[f.column], b = f.value; if (a === null || b === null) return false; switch(f.op) { case '=': return a === b; case '!=': case '<>': return a !== b; case '<': return compareKeys(a,b) < 0; case '>': return compareKeys(a,b) > 0; case '<=': return compareKeys(a,b) <= 0; case '>=': return compareKeys(a,b) >= 0; } });
    if (s.kind === 'insert' || s.kind === 'upsert') {
      if (new Set(s.columns).size !== s.columns.length) throw new Error('Duplicate insert column'); s.columns.forEach(c=>known(schema,c));
      for (const values of s.values) {
        if (values.length !== s.columns.length) throw new Error('Column/value count mismatch');
        const row = Object.fromEntries(schema.columns.map(c=>[c.name,null])); s.columns.forEach((c,i)=>row[c]=values[i]); schema.columns.forEach(c=>check(c,row[c.name]));
        const idx = rows.findIndex(r=>r[pk] === row[pk]); if (idx >= 0 && s.kind === 'insert') throw new Error('Duplicate primary key');
        if (idx >= 0) rows[idx] = row; else rows.push(row);
        if (rows.length > 10000) throw new Error('At most 10000 rows per table');
      } results.push({rowsAffected:s.values.length});
    } else if (s.kind === 'select') {
      if (s.columns !== '*') { s.columns.forEach(c=>known(schema,c)); if (new Set(s.columns).size !== s.columns.length) throw new Error('Duplicate projection column'); }
      let selected = rows.filter(matches);
      if (s.order) { known(schema,s.order.column); selected.sort((a,b) => { const x=a[s.order.column],y=b[s.order.column]; return compareKeys(x,y)*(s.order.desc?-1:1); }); }
      if (s.limit !== null) selected = selected.slice(0,s.limit);
      results.push({rows:selected.map(row=>Object.fromEntries((s.columns === '*' ? schema.columns.map(c=>c.name) : s.columns).map(c=>[c,row[c]])))});
    } else if (s.kind === 'update') {
      if (new Set(s.assignments.map(a=>a.column)).size !== s.assignments.length) throw new Error('Duplicate update column');
      for (const a of s.assignments) {
        const target = known(schema,a.column);
        if (a.column === pk) throw new Error('Primary key updates are not supported');
        if (a.expression.column) {
          const source = known(schema,a.expression.column);
          if (a.expression.op && (!['INTEGER','REAL'].includes(source.type) || typeof a.expression.value !== 'number' || !Number.isFinite(a.expression.value))) throw new Error('Arithmetic requires numeric operands');
        } else check(target,a.expression.value);
      }
      let count = 0;
      tables[s.table] = rows.map(row => { if (!matches(row)) return row; const next = {...row};
        for (const {column,expression:e} of s.assignments) { let value = e.column ? row[e.column] : e.value;
          if (e.op) { if (typeof value !== 'number' || typeof e.value !== 'number') throw new Error('Arithmetic requires numeric operands'); value = e.op === '+' ? value + e.value : value - e.value; }
          check(known(schema,column),value); next[column] = value;
        } count++; return next;
      }); results.push({rowsAffected:count});
    } else if (s.kind === 'delete') { tables[s.table] = rows.filter(r=>!matches(r)); results.push({rowsAffected:rows.length-tables[s.table].length}); }
    else throw new Error('Unknown AST statement');
    if (['insert','upsert','update'].includes(s.kind)) validateIndexes(schema,tables[s.table]);
  }
  return {catalog,tables,results};
}
