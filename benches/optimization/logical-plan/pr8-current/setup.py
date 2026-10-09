import hashlib,io,json,subprocess,tarfile
from pathlib import Path
root=Path(__file__).resolve().parent;repo=Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
versions={'main':'1bfd41bdcc20f78a58b86957302369825a254076','pr8':'6504d61bca4ffc48ca18d90365c068a67a708b45','eager_context':'6504d61bca4ffc48ca18d90365c068a67a708b45','validate_ids':'6504d61bca4ffc48ca18d90365c068a67a708b45','arrow_cast':'6504d61bca4ffc48ca18d90365c068a67a708b45','argument_vec':'6504d61bca4ffc48ca18d90365c068a67a708b45'}
metadata={'source_commits':versions,'rows':1<<20,'groups':256,'null_fraction':'1/7','filter_selectivity':'3/4 before NULL removal','batch_rows':[128,2048,8192],'columns':[3,32],'samples_per_process_case':8,'scope':'single-worker in-memory AggregateOperator sink, group index, scalar evaluation, update, partial state combine, final merge and source output; excludes storage and pipeline scheduling','controls':'Disable one change at a time on current PR #8; effects are not additive. COVAR_POP is absent from all benchmark workloads. Extra wide columns share immutable buffers; columns still have separate ArrayRef slots.'}
for label,sha in versions.items():
 dest=root/label;dest.mkdir(exist_ok=True);source=dest/'roc';source.mkdir(exist_ok=True)
 archive=subprocess.check_output(['git','archive',sha,'src','Cargo.toml','Cargo.lock'],cwd=repo)
 with tarfile.open(fileobj=io.BytesIO(archive))as tar:tar.extractall(source,filter='data')
 p=source/'Cargo.toml';s=p.read_text();s=s.replace('[workspace]\nmembers = ["integrations/datafusion"]\nresolver = "3"\n','');s=s.split('[[bench]]')[0];p.write_text(s)
 before={str(p.relative_to(source)):p.read_bytes() for p in (source/'src').rglob('*.rs')}
 if label=='eager_context':
  p=source/'src/expr/agg/executor.rs';s=p.read_text();a=s.index('        let selected = self');s=s[:a]+'        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());\n'+s[a:]
  needle='                let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());\n';s=s.replace(needle,'',1)
  a=s.index('    fn evaluate_argument(');b=s.index('    pub fn merge',a)
  s=s[:a]+'''    fn evaluate_argument(&self, input: &RecordBatch) -> Result<Option<ArrayRef>> {
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        self.argument.as_ref().map(|argument| argument.evaluate(&executor)?.into_array(input.num_rows())).transpose()
    }

'''+s[b:];p.write_text(s)
 if label=='validate_ids':
  p=source/'src/exec/aggregate.rs';p.write_text(p.read_text().replace('accumulator.update_validated(batch, &self.group_ids, count)?','accumulator.update(batch, &self.group_ids, count)?'))
 if label=='arrow_cast':
  p=source/'src/expr/agg/accumulator.rs';s=p.read_text();a=s.index('    if argument.data_type() == data_type {',s.index('fn cast_argument'));b=s.index('\n}\n',a);s=s[:a]+'    Ok(Cow::Owned(cast(argument.as_ref(), data_type)?))'+s[b:];p.write_text(s)
 if label=='argument_vec':
  p=source/'src/expr/agg/executor.rs';s=p.read_text();s=s.replace('        self.accumulator.update(value.as_ref(), ids)','        let values = value.into_iter().collect::<Vec<_>>();\n        self.accumulator.update(values.first(), ids)');p.write_text(s)
 changes={path:hashlib.sha256((source/path).read_bytes()).hexdigest() for path,content in before.items() if (source/path).read_bytes()!=content};metadata.setdefault('modified_source_files',{})[label]=changes
 (dest/'src').mkdir(exist_ok=True);(dest/'src/main.rs').write_text((root/'main.rs').read_text())
 (dest/'Cargo.toml').write_text('''[package]
name = "pr8-ablation"
version = "0.1.0"
edition = "2024"
[workspace]
members = ["roc"]
resolver = "3"
[features]
baseline = []
[dependencies]
roc = { path = "roc" }
arrow = { version = "=59.3.0", default-features = false }
futures = "0.3"
asyncband = { version = "0.7.3", default-features = false, features = ["shutdown", "mpmc"] }
''')
 (dest/'Cargo.lock').write_bytes((source/'Cargo.lock').read_bytes())
 metadata.setdefault('source_sha256',{})[label]={str(p.relative_to(dest)):hashlib.sha256(p.read_bytes()).hexdigest() for p in list((source/'src').rglob('*.rs'))+[source/'Cargo.toml',dest/'src/main.rs',dest/'Cargo.toml']}
(root/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
