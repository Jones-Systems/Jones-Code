// Dependency-free, sequential test interpreter. It executes production generators and SQL,
// but does not qualify Effect scheduling, interruption, scopes, diagnostics or type safety.
import * as NodeModule from "node:module";
const sources = {
  "effect/Effect": `
export const effect = run => ({run,pipe(...f){return f.reduce((x,p)=>p(x),this)},*[Symbol.iterator](){return yield this}});
export const gen = f => effect(async c=>{const g=f();let r=g.next();while(!r.done){try{r=g.next(await r.value.run(c))}catch(e){r=g.throw(e)}}return r.value});
export const succeed=x=>effect(async()=>x), sync=f=>effect(async()=>f()), fail=e=>effect(async()=>{throw e}), failCause=fail;
const voidEffect=succeed(undefined);export {voidEffect as void};
export const fn=()=> (f,...p)=>(...args)=>p.reduce((e,op)=>op(e,...args),gen(()=>f(...args)));
export const promise=f=>effect(async()=>f());
export const tryPromise=({try:f,catch:g})=>effect(async()=>{try{return await f()}catch(e){throw g(e)}});
export const try_=({try:f,catch:g})=>effect(async()=>{try{return f()}catch(e){throw g(e)}});export {try_ as try};
export const suspend=f=>effect(c=>f().run(c));
export const map=f=>e=>effect(async c=>f(await e.run(c)));
export const tap=f=>e=>effect(async c=>{const v=await e.run(c);await f(v).run(c);return v});
export const flatMap=f=>e=>effect(async c=>f(await e.run(c)).run(c));
export const andThen=n=>flatMap(()=>n), asVoid=e=>map(()=>undefined)(e), as=x=>map(()=>x);
export const catch_=f=>e=>effect(async c=>{try{return await e.run(c)}catch(x){return f(x).run(c)}});export {catch_ as catch,catch_ as catchCause};
export const tapError=f=>e=>effect(async c=>{try{return await e.run(c)}catch(x){await f(x).run(c);throw x}});
export const mapError=f=>e=>effect(async c=>{try{return await e.run(c)}catch(x){throw f(x)}});
export const result=e=>effect(async c=>{try{return {_tag:'Success',success:await e.run(c)}}catch(failure){return {_tag:'Failure',failure}}});
export const ensuring=n=>e=>effect(async c=>{try{return await e.run(c)}finally{await n.run(c)}});
export const uninterruptibleMask=f=>f(e=>e), orDie=e=>e;
export const provideService=(tag,v)=>e=>effect(c=>e.run(new Map([...c,[tag,v]])));
export const provide=layer=>e=>effect(async c=>{if(layer instanceof Map)return e.run(new Map([...c,...layer]));const v=await layer.value.run(c);return e.run(new Map([...c,[layer.tag,v]]))});
export const context=()=>effect(async c=>c), serviceOption=tag=>effect(async c=>c.has(tag)?{_tag:'Some',value:c.get(tag)}:{_tag:'None'});
export const runPromise=e=>e.run(new Map());
`,
  "effect/DateTime": `export const nowUnsafe=()=>new Date();export const toEpochMillis=value=>value.getTime();export const formatIso=value=>value.toISOString();`,
  "effect/Context": `import {effect} from 'effect/Effect';export const Service=()=>()=>class {static of(x){return x}static *[Symbol.iterator](){return yield effect(c=>{if(!c.has(this))throw Error('Missing test service '+this.name);return c.get(this)})}};export const Reference=(_key,options)=>class {static async run(c){return c.has(this)?c.get(this):options.defaultValue()}static *[Symbol.iterator](){return yield effect(c=>this.run(c))}};`,
  "effect/Layer": `export const effect=(tag,value)=>({tag,value});`,
  // Only the finite codec subset used by these sequential fixtures is interpreted.
  // Configured Effect/Schema parsing, encoding, diagnostics and types remain CI proof.
  "effect/Schema": `import {fail as failEffect,try_ as attempt} from 'effect/Effect';
const shape=check=>({check});export const String=shape(x=>typeof x==='string'),Number=shape(x=>typeof x==='number'),Unknown=shape(()=>true),Null=shape(x=>x===null),Defect=()=>Unknown;
export const optional=s=>({...s,optional:true,allowUndefined:true}),optionalKey=s=>({...s,optional:true}),mutable=s=>s,Literal=x=>shape(v=>v===x),Literals=xs=>shape(v=>xs.includes(v));
export const Struct=fields=>shape(x=>x!==null&&typeof x==='object'&&!globalThis.Array.isArray(x)&&Object.entries(fields).every(([k,s])=>!Object.hasOwn(x,k)?s.optional:x[k]===undefined&&s.allowUndefined||s.check(x[k])));
export const Array=s=>shape(x=>globalThis.Array.isArray(x)&&x.every(s.check)),Tuple=ss=>shape(x=>globalThis.Array.isArray(x)&&x.length===ss.length&&ss.every((s,i)=>s.check(x[i]))),Union=ss=>shape(x=>ss.some(s=>s.check(x)));
export const NullOr=s=>shape(x=>x===null||s.check(x));
export const Record=(k,s)=>shape(x=>x!==null&&typeof x==='object'&&!globalThis.Array.isArray(x)&&Object.entries(x).every(([a,b])=>k.check(a)&&s.check(b)));
export const fromJsonString=s=>({check:x=>typeof x==='string',json:s});
const validate=(s,x)=>{if(!s.check(x))throw Error('fixture schema mismatch');return x};
export const decodeSync=s=>x=>s.json?validate(s.json,JSON.parse(x)):validate(s,x),encodeSync=s=>x=>s.json?JSON.stringify(validate(s.json,x)):validate(s,x);
export const decodeEffect=s=>x=>attempt({try:()=>decodeSync(s)(x),catch:e=>e}),encodeEffect=s=>x=>attempt({try:()=>encodeSync(s)(x),catch:e=>e});
export const TaggedError=()=>()=>class extends Error {constructor(v){super(v.reason);Object.assign(this,v)}*[Symbol.iterator](){return yield failEffect(this)}};export const is=c=>v=>typeof c==='function'?v instanceof c:c.check(v);`,
  "effect/sql/SqlError": `import {fail} from 'effect/Effect';export class UnknownError extends Error{constructor(v){super('unknown SQL error');Object.assign(this,v)}};export class SqlError extends Error{constructor(v){super('SQL error');Object.assign(this,v)}*[Symbol.iterator](){return yield fail(this)}};`,
  "effect/sql/SqlClient": `import {Service} from 'effect/Context';export class SqlClient extends Service()('SqlClient'){};`,
  "effect/Semaphore": `export const makeUnsafe=()=>({withPermit:e=>e});`,
};
export function installEffectSeam() {
  NodeModule.registerHooks({
    resolve(specifier, context, next) {
      return sources[specifier]
        ? {
            url: "data:text/javascript," + encodeURIComponent(sources[specifier]),
            shortCircuit: true,
          }
        : next(specifier, context);
    },
  });
}
