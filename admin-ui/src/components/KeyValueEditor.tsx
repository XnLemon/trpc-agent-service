import { Button, Input } from 'tdesign-react';
import { AddIcon, DeleteIcon } from 'tdesign-icons-react';

interface KeyValueEditorProps {
  value: Record<string, string>;
  onChange: (value: Record<string, string>) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
  addLabel?: string;
}

/** Editor for map<string,string> option bags (Catalog-restricted keys). */
export function KeyValueEditor({
  value,
  onChange,
  keyPlaceholder = '键（受服务端 Catalog 限制）',
  valuePlaceholder = '值',
  addLabel = '添加选项',
}: KeyValueEditorProps) {
  // TDesign FormItem unconditionally injects `value` (the field value, often
  // undefined) into custom-component children; treat all inputs defensively.
  const safeValue = value ?? {};
  const entries = Object.entries(safeValue);

  const update = (index: number, nextKey: string, nextValue: string) => {
    const next = entries.map(([key, val], i) => (i === index ? [nextKey, nextValue] : [key, val]));
    onChange(Object.fromEntries(next.filter(([key]) => key.trim() !== '')));
  };

  return (
    <div>
      {entries.map(([key, val], index) => (
        <div className="admin-kv-row" key={index}>
          <Input aria-label={`选项 ${index + 1} 的键`} value={key} placeholder={keyPlaceholder} onChange={(v) => update(index, String(v), val)} />
          <Input aria-label={`选项 ${index + 1} 的值`} value={val} placeholder={valuePlaceholder} onChange={(v) => update(index, key, String(v))} />
          <Button
            shape="square"
            variant="outline"
            theme="danger"
            icon={<DeleteIcon />}
            aria-label={`移除选项 ${index + 1}`}
            onClick={() => {
              const next = { ...safeValue };
              delete next[key];
              onChange(next);
            }}
          />
        </div>
      ))}
      <Button variant="dashed" icon={<AddIcon />} onClick={() => onChange({ ...safeValue, '': '' })}>
        {addLabel}
      </Button>
    </div>
  );
}
