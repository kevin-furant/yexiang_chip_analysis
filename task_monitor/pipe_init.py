#! /usr/bin/env python3
import json
from pathlib import Path
from typing import Any

"""
此步骤用于初始化流程,通过调取task_monitor/config目录下的配置文件,生成流程输入参数config.json
gCap.json: 所有的芯片相关的参数
chip_name_tozh.json: 芯片名称转换成中文名
config.json: 输出的config.json示例
"""
gCap_path = Path(__file__).parent.resolve() / 'config' / 'gCap.json'
chipName_map = Path(__file__).parent.resolve() / 'config' / 'chip_name_tozh.json'

def read_gCap(gcap_path: Path) -> dict:
    with open(gcap_path, 'r', encoding='utf-8') as f:
        gcap_data = json.load(f)
    return gcap_data

def read_chip_name_tozh(chip_name_tozh_path: Path = chipName_map) -> dict:
    with open(chip_name_tozh_path, 'r', encoding='utf-8') as f:
        chip_name_tozh_data = json.load(f)
    return chip_name_tozh_data

def generate_config(out_json_path: Path, **kwargs: Any) -> Path:
    gcap_data = read_gCap(gCap_path)
    chip_id = kwargs.get('chip_id')
    if not chip_id:
        raise ValueError('generate_config 需要传入 chip_id')
    if chip_id not in gcap_data:
        raise KeyError(f'chip_id={chip_id} 不在 gCap 配置中')

    source_chip_config = gcap_data[chip_id]
    if not isinstance(source_chip_config, dict):
        raise TypeError(f'gCap 中 chip_id={chip_id} 的配置不是字典')

    _chip_dict = dict(source_chip_config)
    _chip_dict['chip_id'] = chip_id
    for key, value in kwargs.items():
        if key == 'chip_id':
            continue
        _chip_dict[key] = value

    out_json_path.parent.mkdir(parents=True, exist_ok=True)
    out_json_path.write_text(json.dumps(_chip_dict, indent=4, ensure_ascii=False), encoding='utf-8')
    return out_json_path