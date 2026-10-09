"""Generate factorial controls from pinned production accumulator sources."""
import subprocess,json,hashlib
from pathlib import Path
root=Path(__file__).resolve().parent
metadata=json.loads((root/'metadata.json').read_text());repo=subprocess.check_output(['git','rev-parse','--show-toplevel'],cwd=root,text=True).strip()
def source(sha,path):return subprocess.check_output(['git','show',f'{sha}:{path}'],cwd=repo,text=True)
def span(s,signature):
 i=s.index(signature);opening=s.index('{',i);depth=1;end=opening+1
 while depth:depth+=(s[end]=='{')-(s[end]=='}');end+=1
 return i,end
before=source(metadata['heads']['before'],'src/expr/agg/accumulator.rs');after=source(metadata['heads']['after'],'src/expr/agg/accumulator.rs');legacy=source(metadata['heads']['runtime_reference'],'src/expr/agg/accumulator.rs')
out=root/'src/generated';out.mkdir(exist_ok=True)
(out/'before.rs').write_text(before);(out/'after.rs').write_text(after)
(root/'src/error.rs').write_text(source(metadata['heads']['after'],'src/error.rs'))
agg=source(metadata['heads']['after'],'src/expr/agg/aggregate.rs');a=agg.index('#[derive');b=agg.index('/// An aggregate call');(root/'src/aggregate_function.rs').write_text(agg[a:b])
# All four controls share the after-state layout and carry the same minimum
# field and update pointer. Only update dispatch and comparator selection differ.
common=after.replace('        data_type: DataType,\n    },\n}\n','        data_type: DataType,\n        minimum: bool,\n    },\n}\n',1)
assert common!=after
common=common.replace('                        data_type: output.clone(),\n                    },\n                    if function == Min','                        data_type: output.clone(),\n                        minimum: function == Min,\n                    },\n                    if function == Min',1)
assert common.count('minimum: function == Min')==1
start,end=span(legacy,'    pub fn update(&mut self, values: &[ArrayRef], ids: &[usize])')
runtime=legacy[start:end].replace('match self {','match &mut self.state {',1).replace('Self::','AccumulatorState::').replace('groups.update(&values, ids)?','groups.update_runtime(&values, ids)?')
a,b=span(legacy,'    fn update(&mut self, values: &ArrayRef, ids: &[usize])')
sum_runtime=legacy[a:b].replace('fn update(','fn update_runtime(',1)
point=common.index('impl SumGroups {')+len('impl SumGroups {');common=common[:point]+'\n'+sum_runtime+'\n'+common[point:]
# Dynamic direction uses exactly the old row-comparison body.
a,b=span(common,'fn update_extremum<const MINIMUM: bool>')
dynamic=common[a:b].replace('fn update_extremum<const MINIMUM: bool>','fn update_extremum_dynamic').replace('    let (groups, converter) = state.as_extremum_mut();','''    let AccumulatorState::Extremum { groups, converter, minimum, .. } = state else {
        unreachable!("aggregate state and update function are bound together")
    };''').replace('if MINIMUM {','if *minimum {')
common+='\n'+dynamic+'\n'
for mode in ['enum_dynamic','enum_static','bound_dynamic','bound_static']:
 s=common
 if mode.startswith('enum'):
  body=runtime
  if mode.endswith('static'):
   start=body.index('            AccumulatorState::Extremum {');end=body.index('\n        }\n        Ok(())',start)
   body=body[:start]+'''            AccumulatorState::Extremum { minimum, .. } => {
                if *minimum {
                    update_extremum::<true>(&mut self.state, values, ids)?;
                } else {
                    update_extremum::<false>(&mut self.state, values, ids)?;
                }
            }'''+body[end:]
  a,b=span(s,'    pub fn update(&mut self, values: &[ArrayRef], ids: &[usize])');s=s[:a]+body+s[b:]
 if mode=='bound_dynamic':
  old='''                    if function == Min {
                        update_extremum::<true>
                    } else {
                        update_extremum::<false>
                    },'''
  assert s.count(old)==1;s=s.replace(old,'                    update_extremum_dynamic,',1)
 (out/(mode+'.rs')).write_text(s)
for file in out.glob('*.rs'):
 file.write_text(file.read_text().replace('groups.resize_with(count, HashSet::new)', 'groups.resize_with(count, crate::new_group_set)'))
metadata['generated_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.glob('*.rs'))};(root/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
print('Prepared all six variants from exact source commits')
