"""批量将 Kafka 目录下的 PDF 转为 Markdown"""
import pymupdf4llm, sys, time
from pathlib import Path

KAFKA = Path(__file__).parent / 'Kafka'
pdfs = sorted(KAFKA.glob('*.pdf'))
print(f'共 {len(pdfs)} 个 PDF\n')

ok, fail, empty = [], [], []
for i, pdf in enumerate(pdfs, 1):
    out = pdf.with_suffix('.md')
    t0 = time.time()
    try:
        md = pymupdf4llm.to_markdown(str(pdf))
        if not md.strip():
            empty.append(pdf.name)
            print(f'[{i}/{len(pdfs)}] EMPTY  {pdf.name}  (无文本，可能是扫描件)')
            continue
        out.write_text(md, encoding='utf-8')
        ok.append(pdf.name)
        print(f'[{i}/{len(pdfs)}] OK     {pdf.name}  -> {len(md)} 字符  {time.time()-t0:.1f}s')
    except Exception as e:
        fail.append((pdf.name, str(e)[:80]))
        print(f'[{i}/{len(pdfs)}] FAIL   {pdf.name}  {str(e)[:80]}')

print(f'\n=== 完成 ===')
print(f'成功: {len(ok)}  空文本: {len(empty)}  失败: {len(fail)}')
if empty:
    print(f'\n空文本(疑似扫描件):')
    for n in empty: print(f'  {n}')
if fail:
    print(f'\n失败:')
    for n, e in fail: print(f'  {n}: {e}')
