"""Generate factorial controls from pinned production accumulator sources."""
import subprocess,json,hashlib
from pathlib import Path
root=Path(__file__).resolve().parent
metadata=json.loads((root/'metadata.json').read_text())
def source(sha,path):
 descriptor=metadata['snapshot_sources'][sha+'::'+path]
 data=(root/descriptor['file']).read_bytes()
 assert hashlib.sha256(data).hexdigest()==descriptor['sha256']
 assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==descriptor['git_blob']
 return data.decode()
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
# A single common state and single copy of every loop prevents unrelated
# module/codegen differences from masquerading as dispatch effects.
shared=common
insert=shared.index('    pub fn state_types(',shared.index('impl Accumulator'))
control_methods='    pub fn use_dynamic_direction(&mut self) {\n        if matches!(&self.state, AccumulatorState::Extremum { .. }) {\n            self.update_fn = update_extremum_dynamic;\n        }\n    }\n\n    pub fn update_enum_dynamic(&mut self, values: &[ArrayRef], ids: &[usize]) -> Result<()> {\n        match &mut self.state {\n            AccumulatorState::Count(_) => update_count(&mut self.state, values, ids),\n            AccumulatorState::Distinct { .. } => update_distinct(&mut self.state, values, ids),\n            AccumulatorState::Sum { groups, data_type } => {\n                let argument = values.first().ok_or_else(|| crate::error::Error::Execution("sum requires one argument".into()))?;\n                let values = cast(argument.as_ref(), data_type)?;\n                groups.update_runtime(&values, ids)\n            }\n            AccumulatorState::Avg(_) => update_avg(&mut self.state, values, ids),\n            AccumulatorState::Covar(_) => update_covar(&mut self.state, values, ids),\n            AccumulatorState::Extremum { .. } => update_extremum_dynamic(&mut self.state, values, ids),\n        }\n    }\n\n    pub fn update_enum_static(&mut self, values: &[ArrayRef], ids: &[usize]) -> Result<()> {\n        match &mut self.state {\n            AccumulatorState::Count(_) => update_count(&mut self.state, values, ids),\n            AccumulatorState::Distinct { .. } => update_distinct(&mut self.state, values, ids),\n            AccumulatorState::Sum { groups, data_type } => {\n                let argument = values.first().ok_or_else(|| crate::error::Error::Execution("sum requires one argument".into()))?;\n                let values = cast(argument.as_ref(), data_type)?;\n                groups.update_runtime(&values, ids)\n            }\n            AccumulatorState::Avg(_) => update_avg(&mut self.state, values, ids),\n            AccumulatorState::Covar(_) => update_covar(&mut self.state, values, ids),\n            AccumulatorState::Extremum { minimum, .. } => {\n                if *minimum { update_extremum::<true>(&mut self.state, values, ids) }\n                else { update_extremum::<false>(&mut self.state, values, ids) }\n            }\n        }\n    }\n\n'
shared=shared[:insert]+control_methods+shared[insert:]
# Keep identical kernels out of dispatch wrappers for the factorial comparison.
for name in ['update_count','update_distinct','update_avg','update_covar','update_extremum','update_extremum_dynamic']:
 shared=shared.replace('fn '+name+'(', '#[inline(never)]\nfn '+name+'(')
 shared=shared.replace('fn '+name+'<', '#[inline(never)]\nfn '+name+'<')
shared=shared.replace('    fn update(&mut self, array: &PrimitiveArray<T>', '    #[inline(never)]\n    fn update(&mut self, array: &PrimitiveArray<T>')
(out/'shared.rs').write_text(shared)
for file in out.glob('*.rs'):
 file.write_text(file.read_text().replace('groups.resize_with(count, HashSet::new)', 'groups.resize_with(count, crate::new_group_set)'))
metadata['generated_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.glob('*.rs'))};(root/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
print('Prepared all six variants from exact source commits')
