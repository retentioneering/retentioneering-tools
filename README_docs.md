Гайд по документации
----

Для документирования используется нотация Numpy.

## Примеры docstring
### Cтруктура:

```
"""
Parameters
----------

"""
    Краткое описание, что делает класс/функция.

    Parameters
    ----------
    param1 : type, optional
        Описание первого параметра

    param2 : type, default="group_alias"

    Returns
    -------
    Return type
        Описание возвращаемого результата

    See Also
    --------
        Надо выработать правила для этого блока

    Note
    -------
        Надо выработать правила для этого блока

    Raises
    ------
        Описание, когда выбрасывается ошибка.
        Пока не получилось сделать список внутри этого блока
        Парсится только первый буллит или номер пункта
"""
```
---
### Используемая разметка текста:
```
"""
    *italic*
    **bold**
    ``inline code/object``
    :py:func:`func path` - internal ref
    `text <url>`__ - external link

"""
```

## Сбор доки и основные команды
Установить sphinx и все необходимые расширения из конфига `docs/source/conf.py`

```commandline
pip install sphinx
```
Установить тему `sphinx-rtd-theme`
```commandline
pip install sphinx-rtd-theme
```
```commandline
cd docs
```
Cгенерить доку

```commandline
make html
```

Открыть доку

```commandline
open index.html
```

Очистить папку `_build`
```commandline
make clean
```

## Экспорт картинок в html для раздела "tools description"

```
import plotly.express as px

fig = funnel.draw_plot()
fig.write_html("funnel_0.html")
```
