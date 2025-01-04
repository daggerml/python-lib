import json
import logging
import subprocess
import traceback as tb
from dataclasses import InitVar, dataclass, field, fields
from tempfile import TemporaryDirectory
from typing import Callable, Dict, List, Optional, overload

logger = logging.getLogger(__name__)

DATA_TYPE = {}


def dml_type(cls=None):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return cls
    return decorator(cls) if cls else decorator


def js_dumps(js):
    return json.dumps(js, separators=(',', ':'))


@dml_type
@dataclass(frozen=True, slots=True)
class Resource:
    """An externally managed value (e.g. dataset or otherwise).

    Parameters
    ----------
    uri : str
        The URI of the resource.
    data : str, optional
        The data of the resource. Default is ''.

    Attributes
    ----------
    uri : str
        The URI of the resource.
    data : str
        The data of the resource.
    js : dict
        The data of the resource as a dictionary.
    scheme : str
        The scheme of the URI.
    id : str
        The ID of the resource.
    """
    uri: str
    data: str = ''

    @property
    def js(self):
        return json.loads(self.data)

    @property
    def scheme(self):
        car, cdr = self.uri.split(':', 1)
        return car

    @property
    def id(self):
        car, cdr = self.uri.split(':', 1)
        return cdr

Scalar = str | int | float | bool | type(None) | Resource

@dml_type
@dataclass
class Error(Exception):
    """An error that occurred during execution"""
    message: str
    context: dict = field(default_factory=dict)
    code: str|None = None

    def __post_init__(self):
        self.code = type(self).__name__ if self.code is None else self.code

    @classmethod
    def from_ex(cls, ex):
        if isinstance(ex, Error):
            return ex
        formatted_tb = tb.format_exception(type(ex), value=ex, tb=ex.__traceback__)
        return cls(str(ex), {"trace": formatted_tb}, type(ex).__name__)

    def __str__(self):
        msg = f"dml.Error({self.code}: {self.message!r})"
        if "trace" in self.context:
            sep = "=" * 80
            msg += f"\n{sep}\nTrace\n{sep}\n" + "\n".join(self.context["trace"])
        return msg


@dml_type
@dataclass(frozen=True)
class Ref:
    """A reference to a value in the dml."""
    to: str

    @property
    def id(self):
        return self.to.split('/')[1]

    @property
    def type(self):
        return self.to.split('/')[0]


@dml_type
@dataclass
class FnWaiter:
    """A waiter for a function result."""
    ref: Ref
    cache_key: str
    dump: str
    dag: "Dag"
    resource_data: str

    def get_result(self):
        ref = self.dag._invoke('get_fn_result', self.ref)
        if ref is None:
            return
        assert isinstance(ref, Ref)
        return Node(self.dag, ref)


@dataclass
class FnUpdater(FnWaiter):
    """A waiter for a function result that updates the result."""
    update_fn: Callable[[str, str, str], str|None]

    @classmethod
    def from_waiter(cls, waiter, update_fn):
        f = {k.name: getattr(waiter, k.name) for k in fields(waiter)}
        out = cls(update_fn=update_fn, **f)
        out.update()
        return out

    def update(self):
        resp = self.get_result()
        if resp is not None:
            return resp
        logger.debug('Updater is not finished yet... updating now.')
        try:
            resp = self.update_fn(self.cache_key, self.dump, self.resource_data)
            if resp is not None:
                logger.debug('found non null resp... Loading %r into %r now.', resp, self.dag.tok)
                self.dag.load_ref(resp)
        except Exception as e:
            dag = self.dag.api.new_dag('asdf', 'qwer', dump=self.dump)
            dag.commit(Error.from_ex(e))
        return self.get_result()


def from_data(data):
    n, *args = data if isinstance(data, list) else [None, data]
    if n is None:
        return args[0]
    if n == 'l':
        return [from_data(x) for x in args]
    if n == 's':
        return {from_data(x) for x in args}
    if n == 'd':
        return {k: from_data(v) for (k, v) in args}
    if n in DATA_TYPE:
        return DATA_TYPE[n](*[from_data(x) for x in args])
    raise ValueError(f'cannot `from_data` {data!r}')


def to_data(obj):
    if isinstance(obj, Node):
        obj = obj.ref
    if isinstance(obj, tuple):
        obj = list(obj)
    n = obj.__class__.__name__
    if isinstance(obj, (type(None), str, bool, int, float)):
        return obj
    if isinstance(obj, (list, set)):
        return [n[0], *[to_data(x) for x in obj]]
    if isinstance(obj, dict):
        return [n[0], *[[k, to_data(v)] for k, v in obj.items()]]
    if n in DATA_TYPE:
        return [n, *[to_data(getattr(obj, x.name)) for x in fields(obj)]]
    raise ValueError(f'no data encoding for type: {n}')


def from_json(text):
    return from_data(json.loads(text))


def to_json(obj):
    return js_dumps(to_data(obj))


def _api(*args):
    try:
        cmd = ['dml', *args]
        resp = subprocess.run(cmd, capture_output=True, check=True)
        return resp.stdout.decode().strip()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise Error.from_ex(e) from e


@dataclass
class Api:
    """A dml api client.

    Parameters
    ----------
    config_dir : str, optional
        The configuration directory. Default is None.
    project_dir : str, optional
        The project directory. Default is None.
    initialize : bool, optional
        Whether to initialize the api. Default is False.
    tmpdirs : List[TemporaryDirectory], optional
        The temporary directories. Default is [].
    flags : Dict[str, str], optional
        The flags. Default is {}.

    Attributes
    ----------
    config_dir : str
        The configuration directory.
    project_dir : str
        The project directory.
    tmpdirs : List[TemporaryDirectory]
        The temporary directories.
    flags : Dict[str, str]
        The flags.

    Examples
    --------
    >>> with Api(initialize=True) as api:
    ...     dag = api.new_dag('test', 'asdf')
    """
    config_dir: InitVar[str|None] = None
    project_dir: InitVar[str|None] = None
    initialize: InitVar[bool] = False
    tmpdirs: List[TemporaryDirectory] = field(default_factory=list)
    flags: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self, config_dir, project_dir, initialize):
        if initialize:
            if config_dir is None and 'config-dir' not in self.flags:
                tmpd = TemporaryDirectory()
                self.tmpdirs.append(tmpd)
                self.flags['config-dir'] = tmpd.__enter__()
            if project_dir is None and 'project-dir' not in self.flags:
                tmpd = TemporaryDirectory()
                self.tmpdirs.append(tmpd)
                self.flags['project-dir'] = tmpd.__enter__()
            self.init()
        if config_dir is not None:
            self.flags['config-dir'] = config_dir
        if project_dir is not None:
            self.flags['project-dir'] = project_dir

    def init(self):
        self('repo', 'create', 'test')
        self('project', 'init', 'test')

    @staticmethod
    def _to_flags(flag_dict: Dict[str, str], **kw: str) -> List[str]:
        out = []
        flag_dict = dict(**flag_dict, **kw)
        for k, v in sorted(flag_dict.items()):
            out.extend([f'--{k}', v])
        return out

    def __call__(self, *args, output='text'):
        resp = _api(*self._to_flags(self.flags, output=output), *args)
        if output == 'json':
            return [json.loads(x) for x in resp.split('\n') if len(x) > 0]
        return resp

    def __enter__(self):
        return self

    def cleanup(self):
        for d in self.tmpdirs:
            d.cleanup()

    def __exit__(self, *x, **kw):
        self.cleanup()

    def load(self, dump):
        """Load a reference.

        Parameters
        ----------
        dump : str
            The dumped reference.

        Returns
        -------
        str
            The loaded reference

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     dag_ref = dag.commit(1)
        ...     dump = api.dump(dag_ref)
        ...     _ = api.load(dump)
        """
        resp = self('repo', 'load-ref', dump)
        return resp

    def dump(self, ref):
        """Dump a reference.

        Parameters
        ----------
        ref : Ref
            The reference to dump.

        Returns
        -------
        str
            The dumped reference

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     dag_ref = dag.commit(1)
        ...     dump = api.dump(dag_ref)
        ...     _ = api.load(dump)
        """
        resp = self('repo', 'dump-ref', to_json(ref))
        return resp

    def new_dag(self,
                name: str|None,
                message: str,
                dump: Optional[str] = None) -> "Dag":
        return Dag.new(name, message, dump=dump, api=self)

@dataclass(frozen=True)
class Node:
    """A node in dml.

    Parameters
    ----------
    dag : Dag
        The dag.
    ref : Ref
        An opaque internal reference to the node.

    Attributes
    ----------
    dag : Dag
        The dag.
    ref : Ref
        An opaque internal reference to the node.

    Examples
    --------
    >>> with Api(initialize=True) as api:
    ...     dag = api.new_dag('test', 'asdf')
    ...     node = dag.put(1)
    ...     assert node.value() == 1
    ...     # we can index into nodes
    ...     node = dag.put({'a': 1})
    ...     assert node['a'].value() == 1
    """
    dag: "Dag"
    ref: Ref

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ref.id})'

    def __hash__(self):
        return hash(self.ref)

    def value(self):
        """Get the value of the node.
        
        Notes
        -----
        Anything you do with the value will not be recorded in the dag. You will
        have to call `dag.put` to record whatever output.
        """
        return self.dag._invoke('get_node_value', self.ref)

    def _db_ex(self, fn_name, *x):
        result = self.dag.start_fn(Resource(f'daggerml:op/{fn_name}'), self, *x)
        result = result.get_result()
        assert result is not None
        return result

    def keys(self) -> "Node":
        """Get the keys of the node.

        Returns
        -------
        Node
            The keys of the node.

        Raises
        ------
        Error
            If the node value is not a dictionary

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     node = dag.put({'a': 1})
        ...     assert node.keys().value() == ['a']

        Notes
        -----
        This function raises an error if the node value is not a dictionary.
        """
        return self._db_ex('keys')

    @overload
    def __getitem__(self, key: slice) -> List["Node"]:
        ...
    @overload
    def __getitem__(self, key: str|int) -> "Node":
        ...
    @overload
    def __getitem__(self, key: "Node") -> "Node":
        ...

    def __getitem__(self, key):
        if isinstance(key, slice):
            # Get the start, stop, and step from the slice
            return [self[i] for i in range(*key.indices(len(self)))]
        return self._db_ex('get', key)

    def len(self) -> "Node":
        """Get the length of the node.

        Returns
        -------
        Node
            The length of the node.

        Raises
        ------
        Error
            If the node value is not a list, dictionary, set, or string.

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     assert dag.put([1, 2, 3]).len().value() == 3
        ...     assert dag.put({1, 2, 2}).len().value() == 2
        ...     assert dag.put("asdf").len().value() == 4
        ...     assert dag.put({'a': 1, 'b': 2}).len().value() == 2

        Notes
        -----
        This function raises an error if the node value is not a list, dict,
        set, or string.
        """
        return self._db_ex('len')

    def type(self) -> "Node":
        """Get the type of the node.

        Returns
        -------
        str
            The type of the node (e.g. 'list', 'dict', 'set', 'str').
        """
        return self._db_ex('type')

    def __len__(self):  # python requires this to be an int
        result = self.len().value()
        assert isinstance(result, int)
        return result

    def __iter__(self):
        if self.type().value() == 'list':
            for i in range(len(self)):
                yield self[i]
        elif self.type().value() == 'dict':
            for k in self.keys():
                yield k

    def items(self):
        """Get the items of the node. This is only valid for dictionaries."""
        for k in self:
            yield k, self[k]

@dataclass
class Dag:
    """A dml dag.

    Parameters
    ----------
    tok : str
        The opaque internal reference to the dag.
    api : Api
        The api.
    result : Ref, optional
        The result of the dag (only populated if the dag has finished). Default
        is None.

    Attributes
    ----------
    tok : str
        The opaque internal reference to the dag.
    api : Api
        The api.
    result : Ref
        The result of the dag (only populated if the dag has finished).
    expr : Node
        The expression of the dag (only for fn dags).

    Examples
    --------
    >>> with Api(initialize=True) as api:
    ...     dag = api.new_dag('test', 'asdf')
    ...     node = dag.put(1)
    ...     _ = dag.commit(node)

    Notes
    -----
    The `Dag` class is a context manager. If an exception is raised within the
    context, the dag will be failed with the exception. If no value has been
    committed when exit is called, the dag will be failed with a generic error.
    """
    tok: str
    api: Api
    result: Ref|None = None

    @classmethod
    def new(cls, name: str|None, message: str,
            dump: Optional[str] = None,
            api_flags: Dict[str, str]|None = None,
            api: Optional[Api] = None) -> "Dag":
        """Create a new dag.

        Parameters
        ----------
        name : str
            The name of the dag.
        message : str
            The message of the dag.
        dump : str, optional
            The dump of the dag. Default is None.
        api_flags : Dict[str, str], optional
            The flags for the api. Default is None.
        api : Api, optional
            The api. Default is None.

        Returns
        -------
        Dag
            The new dag.

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = Dag.new('test', 'asdf', api=api)
        """
        if api is None:
            api = Api(flags=api_flags or {})
        extra = [] if dump is None else ['--dag-dump', dump]
        tok = api('dag', 'create', *extra, name, message)
        assert isinstance(tok, str)
        return cls(tok, api)

    @property
    def expr(self) -> Node:
        """Get the expression of the dag (only for fn dags)."""
        ref = self._invoke('get_expr')
        assert isinstance(ref, Ref)
        return Node(self, ref)

    def _invoke(self, op, *args, **kwargs):
        payload = to_json([op, args, kwargs])
        resp = self.api('dag', 'invoke', self.tok, payload)
        data = from_json(resp)
        if isinstance(data, Error):
            raise data
        return data

    def put(self, data) -> Node:
        """Put a value into the dag.

        Parameters
        ----------
        data : Scalar, Node
            The value to put into the dag.

        Returns
        -------
        Node
            The node that was put into the dag.

        Raises
        ------
        ValueError
            If the data is a node from a different dag or if the data is not a
            type daggerml understands.

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     node = dag.put(1)
        ...     assert node.value() == 1
        """
        if isinstance(data, Node):
            if data.dag != self:
                raise ValueError('asdf')
            return data
        resp = self._invoke('put_literal', data)
        assert isinstance(resp, Ref)
        return Node(self, resp)

    def load(self, dag_name) -> Node:
        """Load the result of a dag into this dag.

        Parameters
        ----------
        dag_name : str
            The name of the dag to load.

        Returns
        -------
        Node
            The node that was loaded into the dag.

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     _ = dag.commit(1)
        ...     dag2 = api.new_dag('test2', 'asdf')
        ...     node = dag2.load('test')
        ...     assert node.value() == 1
        """
        resp = self._invoke('put_load', dag_name)
        assert isinstance(resp, Ref)
        return Node(self, resp)

    def start_fn(self, *expr):
        """Start a function execution.
        
        Parameters
        ----------
        expr : List[Node]
            The expression to execute. Expressions are a list of nodes where the
            first node is the function to execute and the rest are arguments to
            the function (like lisp). In this case, the function is a Resource
            object.

        Returns
        -------
        FnWaiter
            The waiter for the function result.

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     node = dag.put(Resource('my-lib:op/add'))
        ...     fnwaiter = dag.start_fn(node, 1, 2)
        ...     assert fnwaiter.get_result() is None
        ...     fndag = api.new_dag('test2', 'asdf', dump=fnwaiter.dump)
        ...     _, *args = fndag.expr.value()
        ...     _ = fndag.commit(sum(args))
        ...     assert fnwaiter.get_result().value() == 3
        """
        expr = [x if isinstance(x, Node) else self.put(x) for x in expr]
        ref, cache_key, dump = self._invoke('start_fn', expr=[x.ref for x in expr])
        rsrc = expr[0].value()
        assert isinstance(rsrc, Resource)
        return FnWaiter(ref, cache_key, dump, self, rsrc.data)

    def commit(self, result) -> Ref:
        """Commit the result of the dag.

        Parameters
        ----------
        result : Scalar, Node, Error
            The result of the dag.

        Returns
        -------
        Ref
            A reference to the dag in DML.

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     dc = dag.commit(1)
        """
        if not isinstance(result, (Node, Error)):
            result = self.put(result)
        if isinstance(result, Node):
            result = result.ref
        resp = self._invoke('commit', result)
        assert isinstance(resp, Ref)
        self.result = resp
        return resp

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = Error.from_ex(exc_val)
            logger.debug('failing dag with error code: %r', ex.code)
            self.commit(ex)

    def load_ref(self, dump):
        """Load a reference into the dag.

        Parameters
        ----------
        dump : str
            The dumped reference.

        Returns
        -------
        str
            The loaded reference

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     dag_ref = dag.commit(1)
        ...     dump = dag.dump(dag_ref)
        ...     _ = dag.load_ref(dump)

        Notes
        -----
        This function is used in function applications to load the result of a
        function application.
        """
        resp = self.api.load(dump)
        return resp

    def dump(self, ref):
        """Dump a reference from the dag.

        Parameters
        ----------
        ref : Ref
            The reference to dump.

        Returns
        -------
        str
            The dumped reference

        Examples
        --------
        >>> with Api(initialize=True) as api:
        ...     dag = api.new_dag('test', 'asdf')
        ...     dag_ref = dag.commit(1)
        ...     dump = dag.dump(dag_ref)
        ...     _ = dag.load_ref(dump)

        Notes
        -----
        This function is used in function applications to dump the result of a
        function application.
        """
        resp = self.api.dump(ref)
        return resp
