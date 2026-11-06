// Dependency-free, sequential test interpreter. It executes production generators and SQL,
// but does not qualify Effect scheduling, interruption, scopes, diagnostics or type safety.
import { registerHooks } from "node:module";
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
export const result=e=>effect(async c=>{try{return {_tag:'Success',value:await e.run(c)}}catch(failure){return {_tag:'Failure',failure}}});
export const ensuring=n=>e=>effect(async c=>{try{return await e.run(c)}finally{await n.run(c)}});
export const uninterruptibleMask=f=>f(e=>e), orDie=e=>e;
export const provideService=(tag,v)=>e=>effect(c=>e.run(new Map([...c,[tag,v]])));
export const provide=layer=>e=>effect(async c=>{if(layer instanceof Map)return e.run(new Map([...c,...layer]));const v=await layer.value.run(c);return e.run(new Map([...c,[layer.tag,v]]))});
export const context=()=>effect(async c=>c), serviceOption=tag=>effect(async c=>c.has(tag)?{_tag:'Some',value:c.get(tag)}:{_tag:'None'});
export const runPromise=e=>e.run(new Map());
`,
  "effect/DateTime": `export const nowUnsafe=()=>new Date();export const toEpochMillis=value=>value.getTime();export const formatIso=value=>value.toISOString();`,
  "effect/Context": `import {effect} from 'effect/Effect';export const Service=()=>()=>class {static of(x){return x}static *[Symbol.iterator](){return yield effect(c=>{if(!c.has(this))throw Error('Missing test service '+this.name);return c.get(this)})}};`,
  "effect/Layer": `export const effect=(tag,value)=>({tag,value});`,
  "effect/Schema": `import {fail as failEffect} from 'effect/Effect';export const String={},Defect=()=>({}),optional=x=>x;export const TaggedError=()=>()=>class extends Error {constructor(v){super(v.reason);Object.assign(this,v)}*[Symbol.iterator](){return yield failEffect(this)}};export const is=c=>v=>v instanceof c;`,
  "effect/sql/SqlClient": `import {Service} from 'effect/Context';export class SqlClient extends Service()('SqlClient'){};`,
  "effect/Semaphore": `export const makeUnsafe=()=>({withPermit:e=>e});`,
};
export function installEffectSeam() {
  registerHooks({
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
