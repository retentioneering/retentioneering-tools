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
