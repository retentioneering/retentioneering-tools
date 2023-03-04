Гайд по документации
----

Для документирования используется нотация Numpy.

## Примеры docstring
### Cтруктура:

```
def func(param1, param2):
"""
    Краткое описание, что делает класс/функция.
    Должно идти сразу после объявления функции без пропуска строчки. Иначе не спарсится.

    Parameters
    ----------
    param1 : type, optional (пробелы вокруг двоеточия - обязательны)
        Описание первого параметра
        Если нужен bullet list:

        - point 1
        - point 2

        Multiline bullet list:
        - | poin 1 текст
          | текст на 2 строке

    param2 : type, default <default_value>

    Returns
    -------
    Return type
        Описание возвращаемого результата

    See Also
    --------
    Надо выработать правила для этого блока

    Notes
    -----
    Надо выработать правила для этого блока

    Raises
    ------
    Описание, когда выбрасывается ошибка.
    Пока не получилось сделать список внутри этого блока
    Парсится только первый буллит или номер пункта

   Тут важно, чтобы подчеркивание было не меньше текста
"""
```

### Типы:
```
def func(
        param1: list[str]
        param2: list[str] | None = None
        param3: Literal["open", "closed"] = "open",
        param4: bool = False
):
"""
    Description

    Parameters
    ----------
    param1 : list of str
    param2 : list of str, optional
    param3 : {"open", "closed"}, default "open"
    param4 : bool, default False

"""
```


---
### Используемая разметка текста:
```
"""
    *italic*
    **bold**
    ``inline code/object``
"""
```

### Внутренние ссылки:

```
    :py:func:`func path`
```
### Внешние ссылки:

```
    :numpy_link:`DATETIME_UNITS<>`
    numpy_link - короткое название ссылки
    DATETIME_UNITS - текст, который будет отражаться в доке
    Полные ссылки хранятся в словаре в conf.py

```

## Сбор доки и основные команды
Установить sphinx и все необходимые расширения из конфига `docs/source/conf.py`

```commandline
pip install sphinx
```
Установить тему `pydata_sphinx_theme`
```commandline
pip install pydata_sphinx_theme
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
Код используется в ноутбуке, для сохранения plotly картинок в html
Затем их нужно сохранить в docs/source/_static/tool_directory

```
import plotly.express as px

fig = funnel.draw_plot()
fig.write_html("funnel_0.html")
```

Далее на них можно ссылаться из нужного блока в rst файле

```
.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_0.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>
```


.. _anchor name:
:ref:`Eventstream concept<anchor name>`
