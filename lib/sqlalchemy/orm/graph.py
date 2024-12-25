from typing import Union, Optional

from sqlalchemy import inspect
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept


class RelationshipGraph:
    def __init__(self, base, support_plurality: bool = False):
        import networkx as nx
        self.support_plurality = support_plurality

        self.relationship_graph = nx.DiGraph(directed=True)

        for class_ in base.registry.mappers:
            class_ = class_.class_
            self.relationship_graph.add_node(class_)
            inspector = inspect(class_)
            for rel in inspector.relationships:
                plurality = str(rel.direction).lower().endswith("many")
                self.relationship_graph.add_edge(
                    class_, rel.entity.class_, attribute=rel.key, plurality=plurality
                )


    def has_path(self, start: Union[DeclarativeMeta, DeclarativeAttributeIntercept],
                 end: DeclarativeAttributeIntercept) -> bool:
        return self.get_path(start, end) is not None

    def get_path(self,
                 start: Union[DeclarativeMeta, DeclarativeAttributeIntercept],
                 end: DeclarativeAttributeIntercept, raise_err: bool = False) -> Optional[list[str]]:
        """
        Returns a list of attributes that are need to be traversed to
        get from start to end.
        If plurality is supported, the graph will not return a path
        from Many-to-Many relationships or Many-to-One relationships.

        >>> from sqlalchemy.ext.declarative import declarative_base
        >>> from sqlalchemy import Column, Integer, ForeignKey
        >>> from sqlalchemy.orm import relationship
        >>> Base = declarative_base()
        >>>
        >>> class Grandparent(Base):
        >>>    __tablename__ = "grandparent"
        >>>    id = Column(Integer, primary_key=True)
        >>>    parent = relationship("Parent")
        >>>
        >>> class Parent(Base):
        >>>    __tablename__ = "parent"
        >>>    id = Column(Integer, primary_key=True)
        >>>    grandparent_id = Column(Integer, ForeignKey("grandparent.id"))
        >>>    grandparent = relationship("Grandparent")
        >>>    child = relationship("Child")
        >>>
        >>> class Child(Base):
        >>>    __tablename__ = "child"
        >>>    id = Column(Integer, primary_key=True)
        >>>    parent_id = Column(Integer, ForeignKey("parent.id"))
        >>>    parent = relationship("Parent")
        >>>
        >>> graph = RelationshipGraph(Base)
        >>> path_ = graph.get_path(Child, Grandparent)
        ['parent', 'grandparent']
        >>> path_ = graph.get_path(Grandparent, Child, raise_err=False)
        None
        >>> from sqlalchemy.orm import Relationship
        >>> path_ = graph.get_path(Grandparent, Child)
        ['parent', 'child']

        :param start: The starting node. Can either be a DeclartiveMeta or a DeclarativeAttributeIntercept
        :param end: The ending node. Must be a DeclarativeAttributeIntercept
        :param raise_err: Raise an error if no path is found
        :return: List[str] or None
        """
        import networkx as nx

        if self.relationship_graph is None:
            raise ValueError("The graph has not been built yet.")

        if isinstance(start, DeclarativeMeta):
            start = start.__class__

        if not self.relationship_graph.has_node(start):
            raise ValueError(f"The node {start} is not in the graph.")
        if not self.relationship_graph.has_node(end):
            raise ValueError(f"The node {end} is not in the graph.")

        try:
            path = nx.shortest_path(self.relationship_graph, start, end)
        except nx.NetworkXNoPath as e:
            if raise_err:
                raise e
            return None
        return [
            self.relationship_graph.get_edge_data(path[i], path[i + 1])["attribute"]
            for i in range(len(path) - 1)
        ]

    def traverse(self,
                 start: DeclarativeMeta,
                 end: DeclarativeAttributeIntercept,
                 raise_err: bool = False
    ) -> Optional[Union[DeclarativeMeta, list[DeclarativeMeta]]]:
        """
        Traverses the graph from start to end.
        :param start: An instance of a DeclarativeMeta
        :param end: Tbe ending node. Must be a DeclarativeAttributeIntercept
        :param raise_err: Raise an error if no path is found
        :return: A DeclarativeMeta or a list of DeclarativeMeta
        """
        path = self.get_path(start, end, raise_err=raise_err)
        if path is None:
            return None
        instances = [start]
        for attr in path:
            instances = [getattr(x, attr) for x in instances]
        return instances if self.support_plurality else instances[0]
